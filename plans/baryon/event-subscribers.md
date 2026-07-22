# Specification: Event Subscribers

The engine-level subscriber layer over the event bus of `plans/engine-event-bus.md` (item 6, closed).
This spec turns the bus's empty subscriber table into a real dispatch layer — registration, delivery,
initialization, outcomes, and recovery — while keeping the **registry and coordinator contracts
language-neutral**: no subscriber-specific callback type, VM ABI, or pattern language. The run loop
continues to use its existing payload-type router at the outer integration seam.
`plans/rune-autocmds.md` (the item-9 autocmd slice) and background-job completions (item 7) become
thin bindings over this contract.

Inherited and not reopened: the bus's delivery semantics (post-commit, owned payloads, FIFO,
barrier-absorbing `EventsLost`, next-pass deferral, `MAX_DRAIN_PASSES` + `DrainEvents` self-wake), the
runtime-registry owner model (`RegistryOwnerId` never reused, atomic `replace_owner_generation`,
`remove_owner` — `runtime_registry.rs:630,682,714`), the lease/gate invocation machinery
(`ActionLease`/`InvocationGate::invoke_dynamic<C,E>`, `runtime_registry.rs:57,1597,1602`), the
non-reentrant-VM rule (`rune-scripting.md` §2.4), and the recorded hard prerequisite: **no subscriber
ships until a complete read-only resynchronization surface exists** — which this plan builds.

## 1. Ground truth this builds on

- **The drain invokes a closure per event with no pass boundary exposed**
  (`drain_engine_events(queue, internal, dispatch)`, `event_bus.rs:174`); production dispatch is a
  no-op. Events fired during a pass structurally land in the next pass.
- **The registry overlay already owns dynamic-callback lifecycles and language neutrality**:
  capability registrations under a `RegistryOwnerId`, staged in a bounded atomic `RegistrationBatch`
  (`runtime_registry.rs:248`, with transaction/entry accounting), published by
  `replace_owner_generation`, torn down by `remove_owner`. Commands store an **`ActionLease`** whose
  **typed payload** (`action::<T>()`, `runtime_registry.rs:1488`) carries the owner-side callback
  token — the engine never learns what a callback is. Registry resolution constructs the owned
  invocation snapshot; **`InvocationGate::invoke_dynamic<C,E>`** then enforces non-reentrancy and
  executor-side owner liveness. The run loop routes a lease's invocation by **payload type**: a
  `RuneCallbackRef` payload goes to the thread-local `!Send` `RuneHost` local in `run_engine`;
  anything else goes to the boxed `dynamic_action_executor` fallback (embedders). This existing
  outer router is binding-aware; the registry and subscriber coordinator are not.
- **Baseline state is split across two owners**: buffers/views/focus live in `EditorSession`
  (`capture_text_lifecycle` already enumerates exactly the text-projected view); **sources live in
  `RuneHost`** (`SourceSlot { id: SourceId, current_owner }`, host-private). Events already carry
  only opaque `SourceId`/`RegistryOwnerId`.
- **Outcome reporting today is dispatcher-side**: there is no engine-level message log — command
  outcomes update `ui.status_message` and the Rune host logs to its own `MessageLog`
  (`runtime.rs:2368` region). This plan follows that split rather than inventing an engine sink.

## 2. Resynchronization baseline — composed, read-only, ownership stays put

The load-bearing decision: the baseline is **composed from per-domain contributors; no state moves.**

```
Baseline {
  buffers: Vec<{ buffer: BufferId, state: StateId }>,          // ascending BufferId
  views:   Vec<{ view: ViewId, buffer: BufferId }>,            // session-owned, layout order
  focused: Option<ViewId>,                                     // session-owned, text-projected
  sources: Vec<{ source: SourceId, owner: RegistryOwnerId }>,  // contributed, ascending SourceId
}
```

- **The session provides the buffer/view/focus part directly** — exactly
  `capture_text_lifecycle`'s projection, exposed read-only: same text-only scope, same layout
  traversal order, same focus projection as the events, so a baseline plus subsequent events is a
  coherent fold by construction.
- **Sources are contributed, not centralized.** The bus/engine never learns `SourceKey`, paths, or
  host internals. A **`BaselineContributor`** seam implemented by `RuneHost` appends live
  `(SourceId, RegistryOwnerId)` pairs to a bounded builder in **ascending `SourceId` order**. The run
  loop passes the thread-local host as `Option<&dyn BaselineContributor>` while assembling; no
  contributor means an empty source list (headless embedders without a script host). The contributor
  is not stored in `Engine` and therefore does not cross the `Send` boundary that `RuneHost` cannot
  cross.

  ```
  trait BaselineContributor {
    fn contribute(&self, builder: &mut BaselineBuilder) -> Result<(), BaselineError>;
  }
  ```
- **Read-only is structural**: contributors take `&self`; assembly runs **outside** any callback
  frame and never exposes a mutable session/host.
- **Bounded and fallible**: a `BaselineBuilder` enforces `MAX_BASELINE_VIEWS`,
  `MAX_BASELINE_BUFFERS`, and `MAX_BASELINE_SOURCES` (§8), rejects duplicates and out-of-order source
  contribution, and uses `try_reserve` before materializing each vector. The session collector stops
  at cap + 1 rather than traversing an unbounded layout after failure; the contributor cannot bypass
  the builder's caps. Exceeding a cap or failing allocation returns `BaselineError` and leaves every
  affected subscription in `NeedsBaseline` (§6); it never constrains ordinary editor operation or
  aborts the process.
- **No retrospective transition events fire — but baseline initialization is always attempted.** A
  new subscriber never receives synthesized `BufferOpened`-style events for pre-existing state. On
  successful initialization it accepts one baseline (§5) and then transitions; failed attempts stay
  outside ordinary delivery and follow §6's bounded policy.

## 3. Registration — owner-scoped, atomic, kind-filtered, lease-carried

- **A subscription is a registry capability**, exactly like a runtime command: staged through the
  same `RegistrationBatch` (counting toward its existing **transaction and session entry
  accounting**), published atomically with the rest of the owner's generation by
  `replace_owner_generation`, removed wholesale by `remove_owner`. A source reload therefore
  atomically swaps its subscriptions with its commands/options/keymaps; events the old generation
  would have seen between teardown and the new generation's first live pass are **absorbed by the
  replacement's baseline** — the new generation initializes from current state, so nothing is
  double-applied or lost.
- **Registration payload**: `{ kinds: EventKindSet, lease: ActionLease }`. The core stores an
  **`ActionLease`** — not a subscriber-specific callback type — and the binding supplies its typed
  payload (`RuneCallbackRef` for the Rune host) exactly as commands do. Language neutrality is
  inherited, not re-invented.
- **`EventKindSet` is an explicit, stable enum mapping**, not "bitset over discriminants": an
  engine-owned `EventKind` enum with one explicitly listed variant per subscribable event
  (`BufferOpened`, `BufferClosed`, `ViewCreated`, `ViewRetargeted`, `ViewClosed`,
  `FocusedTextViewChanged`, `BufferEdited`, `BufferReplaced`, `SourceLoaded`, `SourceUnloaded`,
  `HydrationCompleted`), each with a fixed bit position that is append-only across versions. Coarse
  kind filtering is engine-level (skip resolution and dispatch entirely for unmatched kinds);
  anything finer (buffer filters, glob patterns) is the binding's concern. **`EventsLost` has no
  `EventKind`** — the barrier is protocol (§6), not a subscribable event.
- **Every published subscription gets a monotonic, never-reused `SubscriptionId`.** Publication
  first preflights that one contiguous id range exists for the entire candidate batch without
  advancing the allocator; only the infallible commit assigns those ids and advances the counter.
  Exhaustion therefore rejects before registry mutation, and failed publication consumes no ids.
  This is the identity dispatch snapshots hold (§4): `(owner, index)` would ABA under same-owner
  replacement; `SubscriptionId` cannot.
- **Caps preflighted at staging**: `MAX_SUBSCRIBERS_PER_OWNER` and `MAX_SUBSCRIBERS_TOTAL` (§8)
  reject at `RegistrationBatch` admission — never mid-publish, preserving batch atomicity. Session
  totals use the overlay's existing subtract-replaced-owners/add-candidate algebra, so generation
  replacement never transiently double-charges old and new subscriptions. Subscription entries also
  count toward `MAX_TRANSACTION_ENTRIES`/`MAX_SESSION_ENTRIES`, which bounds their fixed storage.
  Like command action payloads, executor-owned lease payload memory is excluded from registry
  metadata bytes and remains the host's retained-memory responsibility. An empty `EventKindSet` is
  rejected during the same preflight: this layer has no baseline-only subscription category.

## 4. Dispatch — capture first, then initialize and deliver

**The drain API grows a pass boundary.** Its current per-event closure cannot express initialization
or a pass-scoped subscriber snapshot. More importantly, `take_pass` must happen **before any
subscriber callback**: otherwise an event fired by an initializer would be captured into the pass
about to be delivered, violating the bus's next-pass rule.

```
for each boundary (bounded by MAX_DRAIN_PASSES when callbacks/events are serviced):
    1. capture:    take_pass, swapping the pending queue out before any callback
    2. barrier:    if the captured pass is EventsLost, demote every subscriber;
                   the marker has no ordinary delivery
    3. snapshot:   collect (id, kinds) for entries Live before initialization
    4. initialize: snapshot NeedsBaseline ids, assemble one baseline, initialize them;
                   they are excluded from the step-3 snapshot even on success
    5. deliver:    for each non-marker event, for each snapshotted id whose kinds match:
                     re-resolve the id against the live table; skip if gone/disabled;
                     invoke through the shared gate; interpret the outcome
```

The coordinator services an existing **`PendingSubscriberPass` before capturing any newer queue
records**. A newly captured pass becomes one owned pending record containing its events, copied
`(SubscriptionId, EventKindSet)` live snapshot, copied `SubscriptionId` initialization snapshot,
current event/subscriber and initialization cursors, and—while initialization is incomplete—one
owned baseline. Both snapshots are sorted by ascending `SubscriptionId`. This is required for
bounded fan-out: `MAX_SUBSCRIBER_STEPS_PER_DRAIN` charges one step for each initialization candidate
and each event × subscriber candidate **before filtering or resolution**, so unmatched floods are
bounded too; actual callback entries are therefore bounded by the same ceiling. The counter is
initialized once per top-level drain call and shared across every captured pass serviced by that
call—it is not reset at each `MAX_DRAIN_PASSES` iteration. On exhaustion, the coordinator retains its
cursors, attempts a `DrainEvents` self-wake, returns the accumulated render/status outcome, and
resumes that exact pass before any later event. A full request channel is safe for the same reason as
the bus self-wake: an already-queued request will reach the drain point. Only one pending subscriber
pass exists, and all of its owned vectors are bounded by the queue/baseline/subscriber caps.

Snapshot storage is fallible too, but its capacity is reserved **before** `take_pass`: the coordinator
counts the currently published subscriptions, `try_reserve`s room for the bounded live and
initialization snapshots, and only then captures the bus queue. No callback or cross-thread bus push
can intervene on the engine thread. If reservation fails, the bus remains untouched, no delivery
state changes, and the coordinator returns a fixed `BoundedStatus`; it retries only at a later
external drain boundary, not through an immediate self-wake loop. Once capture occurs, both frozen
id sets populate within reserved capacity and cannot fail, so an event pass is never captured without
its original subscriber cutoff.

Revision-neutral delivery state (`NeedsBaseline`/`Live`/`Disabled` and failure counts) is co-located
with each subscription entry inside `RuntimeRegistries`; it is mutable runtime state but not part of
declarative equality/accounting. The optional `PendingSubscriberPass` lives in a session-owned
`SubscriberRuntime` beside the registry and event queue and holds ids only—never a second liveness
table. Owner removal/replacement drops registry entries immediately; stale pending ids simply fail
re-resolution.

This slice is part of ordering, not a parallel queue: requests may commit newer state before a
deferred slice resumes, but their events remain in the live bus queue. Every subscriber still sees
the older pending pass first. Stable ids are re-resolved on resumption, so owner removal while the
slice was deferred suppresses stale delivery. Registrations made meanwhile cannot join the frozen
snapshot and initialize only after the pending pass completes. An owned baseline is likewise an
immutable snapshot of its original boundary: a delayed initializer must treat it as authoritative
even if live editor queries already reflect later requests; those later transitions remain queued
behind the pending pass.

The coordinator runs an **empty-queue boundary** when any subscription needs initialization, so a
registration with no event queued still receives its baseline. If that boundary leaves the live
queue empty, the drain returns even when initialization failed; retries happen on later external
drain calls rather than spinning three times in one idle call. Events fired during initialization or
delivery land in the fresh queue and can only enter the next captured pass. A boundary that invokes
initializers counts toward `MAX_DRAIN_PASSES`, just like one that delivers events.

- **Snapshots hold `(SubscriptionId, EventKindSet)`, never references or callback copies.** The
  fixed-size kind set is copied so an unmatched event skips registry resolution entirely. Callbacks
  receive mutable access to the same registry, so borrowed snapshots are impossible; copying a lease
  would defeat immediate removal. For a matching kind, re-resolution clones a fresh lease into a
  `SubscriberInvocation` only after confirming the exact id is still published and eligible. A
  removed/replaced id fails resolution and is skipped; never-reused ids prevent an impostor.
- **The registry remains language-neutral.** A resolved `SubscriberInvocation` owns the
  `SubscriptionId`, owner, and cloned `ActionLease`, and exposes the same `action::<T>()` downcast as
  `InvocationSnapshot`. The language-neutral executor seam is:

  ```
  SubscriberDispatcher<C> {
    fn owner_is_live(&self, owner: RegistryOwnerId) -> bool;
    fn initialize(&mut self, context: &mut C, invocation: SubscriberInvocation,
                  baseline: &Baseline) -> SubscriberOutcome;
    fn deliver(&mut self, context: &mut C, invocation: SubscriberInvocation,
               event: &EngineEvent) -> SubscriberOutcome;
  }
  SubscriberOutcome = { needs_render: bool,
                        foreground_status: Option<BoundedStatus>,
                        result: Delivered | Failed }
  ```

  `initialize` is a **replace-baseline** operation, never a merge and never a synthetic event. It
  may be called again after failure or `EventsLost`; a successful return asserts that the
  subscriber's model now corresponds exactly to the supplied snapshot.

  `BoundedStatus` is an engine-owned, construction-bounded type capped by
  `MAX_SUBSCRIBER_STATUS_BYTES`; an executor cannot return an unbounded `String` and merely claim it
  was checked. The dispatcher logs detailed errors on its own side (Rune uses its `MessageLog`); the
  engine only merges render and bounded foreground status. Outcome folding is deterministic:
  `needs_render` is ORed, and the last non-`None` foreground status in invocation order wins (the
  same foreground behavior as a command batch); surfacing any status requests a render.
- **One non-reentrancy gate.** `InvocationGate` is factored to a shared guarded-entry primitive;
  both `invoke_dynamic` and the new subscriber entry use the same `active` cell. Subscriber
  invocation performs two checks: exact subscription-id resolution against `RuntimeRegistries`, and
  executor-side owner liveness (the callback store may already have retired the owner). A subscriber
  therefore cannot synchronously invoke any other dynamic/subscriber callback. Gate/routing errors
  are normalized to `Failed` with a fixed `BoundedStatus`; during initialization they increment the
  same failure state as a dispatcher-reported failure, and during ordinary delivery they follow the
  continue-the-pass rule.
- **Concrete run-loop routing.** `Engine` carries a Send
  `Box<dyn SubscriberDispatcher<EditorSession> + Send>` fallback, parallel to its dynamic-action
  fallback; the default rejects invocation. The later Rune binding implements
  `SubscriberDispatcher<EditorHost>` on the thread-local `RuneHost`. At delivery, a Rune payload is
  routed to that host with `&mut EditorHost`; every other payload goes to the boxed fallback with
  `&mut editor.session`. No non-Send dispatcher crosses `thread::spawn`. In this engine-subscriber
  cut, recognizing a Rune-typed subscription must return a bounded "binding unavailable" failure
  rather than fall through to the boxed dispatcher; `rune-autocmds.md` supplies the actual
  `RuneHost` implementation and event ABI.
- **`Failed` ordinary delivery does not unsubscribe or stop the pass.** One broken subscriber must
  not silence siblings. Initialization failure is stateful and handled by §6.
- **Per-entry resource limits remain dispatcher-owned.** Every `initialize` and `deliver` entry must
  establish its binding's instruction/allocation/host-operation budgets and return its render effects
  in `SubscriberOutcome`; the Rune binding reuses the callback accounting protocol from
  `rune-vm-lifecycle.md`, including signed retained-memory deltas on failure. The engine's
  `MAX_SUBSCRIBER_STEPS_PER_DRAIN` is an aggregate scheduling bound, not a replacement for those
  per-callback limits.
- **Order**: queue order within the captured pass; per event, ascending `SubscriptionId`. New ids are
  later than every previously published id, so order is deterministic (replacement intentionally
  receives new ids and therefore a new registration position).

## 5. Subscription lifecycle — `NeedsBaseline | Live | Disabled`

Every subscription carries a persistent delivery state:

- **Publication → `NeedsBaseline { consecutive_failures: 0 }`.** No ordinary events are delivered
  in this state. The failure count is runtime delivery state, not declarative registry metadata and
  does not change the registry revision.
- **Initialization runs after pass capture (coordinator step 4).** The coordinator snapshots the
  ids currently in `NeedsBaseline`, assembles one composed baseline, and invokes each id through the
  same gate/routing. A subscription registered by an initializer is absent from that snapshot and
  waits for the next boundary. After each outcome, state is updated only if the exact
  `SubscriptionId` still resolves; self-removal/replacement wins over the stale result.
- **Success → `Live`, eligible from the *next captured pass* — never the pass already captured.** The
  cutoff direction matters: a baseline assembled at the pass-N boundary already reflects the
  committed effects of every event *queued in pass N* (events are post-commit facts; the state they
  describe exists before they are delivered). Delivering pass N to a subscriber initialized at that
  boundary would double-apply those transitions. The eligible-id snapshot was therefore taken before
  initialization; newly initialized ids first participate when the fresh queue is captured.
- **Failure → remains `NeedsBaseline`** (§6). Ordinary events stay suppressed — a subscriber that
  never absorbed a baseline must not receive increments it cannot anchor.
- **`Disabled` remains published but is never resolved for initialization or delivery.** Disabling
  one subscription must not surgically delete an entry from `OwnerEntries`: that would silently
  change registry accounting/revision and violate whole-owner publication. The declaration and its
  caps remain until owner reload/removal; introspection may report the disabled state. Reload creates
  fresh ids in `NeedsBaseline` and naturally clears the condition.
- Generation replacement re-enters the lifecycle from the top: the new generation's subscriptions
  are `NeedsBaseline` at publication; the old generation's ids stop resolving immediately (§4).
  Registration performed *by a callback mid-pass* publishes normally and is picked up by the next
  boundary's initialization snapshot — registration is effectively a deferred effect, consistent
  with the bus's next-pass rule, and its baseline can never predate events it will later receive.

## 6. Barrier recovery and initialization failure — one mechanism

`EventsLost` and initialization converge on the same state machine:

1. `take_pass` captures the marker before any callback. Because a pending marker is the queue's only
   record, there are no ordinary events to deliver from that pass. Every `Live` subscription is
   demoted to `NeedsBaseline { consecutive_failures: 0 }`; `Disabled` entries remain disabled.
2. Step 4 of that same boundary assembles one composed baseline and initializes the resulting
   `NeedsBaseline` snapshot. Successes become eligible only for the next captured pass. Missed events
   are absorbed by the baseline; events fired during recovery enter the fresh queue and follow it
   exactly once.
3. **A dispatcher-reported initialization failure** increments that exact id's consecutive count
   after re-resolution. Below `MAX_BASELINE_ATTEMPTS` it remains `NeedsBaseline`; at the limit it
   becomes `Disabled` (§5), still charged to and owned by its published generation. The dispatcher
   logs its detailed failure and may return a bounded foreground status. Re-registering or owner
   reload creates a fresh id and starts at zero.
4. **A `BaselineError` before callbacks** applies the same attempt to every snapshotted
   `NeedsBaseline` id, with exact-id re-resolution before updating state. Since no dispatcher ran,
   the coordinator may surface only a fixed `BoundedStatus` describing baseline construction
   failure; no unbounded allocation detail enters UI state. This gives cap/allocation failures the
   same bounded retry/disable behavior and prevents permanent per-request work. These per-id state
   updates use the pending initialization cursor and consume subscriber steps, so even a global
   failure over 4 096 subscribers remains sliced.
5. A recovery callback can overflow the **fresh** queue and synthesize another marker. It is not
   stacked with the captured marker: it waits in the live queue, is captured as the next pass, and
   legitimately triggers another recovery cycle.

## 7. Testing (engine-level, a recording dispatcher — no VM callback execution)

- Registration atomicity: over-cap batches reject at admission with nothing published;
  `replace_owner_generation` swaps subscriptions atomically — old ids stop resolving immediately,
  new subscriptions initialize from current state (absorbed, not double-applied), no event reaches
  both generations; `remove_owner` stops delivery mid-pass via failed re-resolution. Exact-boundary
  tests cover contiguous `SubscriptionId` reservation and prove exhaustion fails before registry
  state or the allocator moves; an empty `EventKindSet` rejects without publication.
- Identity: `SubscriptionId`s are monotonic and never reused across same-owner generation
  replacement (the ABA case: same owner, same index, new batch — old snapshot id resolves to nothing,
  not the new subscription). Subscription leases with the wrong owner reject during batch preflight.
- Lifecycle/cutoff: a subscription registered while pass-N events are queued receives the baseline
  at the pass-N boundary and **none of pass N's events**, then pass-N+1 events exactly once; a
  subscription registered with an **empty queue** still initializes at the next drain; a `Live`
  subscriber delivers normally. An initializer that fires an event proves it enters the fresh queue
  and is not present in the already-captured pass.
- Dispatch: queue order × ascending-`SubscriptionId` order; a `Failed` delivery doesn't stop
  siblings or later deliveries; `needs_render` ORs into the frame decision; unmatched kinds skip
  resolution entirely; the `EventKind` mapping is exhaustively matched against `EngineEvent` (a new
  bus kind without a mapping fails to compile). Subscriber and dynamic action entry share one gate:
  attempted synchronous cross-invocation is rejected and the guard resets after failure.
- Fan-out slicing: a pass whose initialization or event × subscriber work exceeds
  `MAX_SUBSCRIBER_STEPS_PER_DRAIN` stops at the exact cursor, requests a wake, renders any
  accumulated effect, and resumes before newer queued events. Registration cannot join the frozen
  snapshot; removal before resumption suppresses the stale id; a full request channel still
  guarantees eventual progress. Snapshot-allocation failure leaves the bus uncaptured and delivery
  state unchanged, surfaces a bounded status, and retries only on a later external drain. A baseline
  retained across slices stays the authoritative old cut while newer transitions queue behind it.
- Barrier: overflow demotes all `Live` subscribers; in the successful-recovery case each gets
  exactly one baseline (not the marker); post-marker events deliver exactly once after promotion; a subscriber whose
  initialization fails stays suppressed through subsequent passes and is disabled after
  `MAX_BASELINE_ATTEMPTS`, leaving its owner's other subscriptions untouched and the registry's
  counts/revision unchanged. A recovery callback that overflows the fresh queue triggers a second
  recovery on the next captured pass, never recursively.
- Baseline composition: session part equals `capture_text_lifecycle`'s projection; sources ascend
  by `SourceId`, buffers ascend by `BufferId`, and views retain layout order; absent contributor →
  empty sources; every count cap accepts exactly its boundary and rejects boundary + 1 before
  materialization; allocation/contributor failure follows the bounded initialization-failure policy,
  not an abort; assembly takes `&self` everywhere (compile-time).
- Routing/outcomes: Rune-typed payloads never fall through to the boxed dispatcher; non-Rune
  payloads never reach `RuneHost`; a dispatcher cannot construct an oversized `BoundedStatus`.
- No-retrospective-events: pre-existing state produces no transition events and one successful
  initialization baseline.

## 8. Constants (proposed; calibrate with the binding's real callbacks)

| Limit | Value | Basis |
|---|---|---|
| `MAX_SUBSCRIBERS_PER_OWNER` | 256 | a plugin registering more is a bug; batch-admission checked |
| `MAX_SUBSCRIBERS_TOTAL` | 4 096 | matches `MAX_PENDING_EVENTS` scale; bounds per-event fan-out |
| `MAX_SUBSCRIBER_STEPS_PER_DRAIN` | 256 | bounds all fan-out candidates; resumable cursor preserves order |
| `MAX_BASELINE_VIEWS` | 16 384 | matches the runtime registry's session-entry scale; bounded collector |
| `MAX_BASELINE_BUFFERS` | 16 384 | attached text buffers cannot exceed views; separately asserted |
| `MAX_BASELINE_SOURCES` | 4 096 | equals the existing `MAX_SOURCE_SLOTS` contract |
| `MAX_BASELINE_ATTEMPTS` | 3 | consecutive initialization failures before the subscription is disabled |
| `MAX_SUBSCRIBER_STATUS_BYTES` | 4 KiB | matches other foreground dynamic-status bounds; structural newtype |

## 9. Non-goals / deferred

- **The Rune binding** — `baryon::autocmd(...)`, pattern/buffer filtering, autocmd ergonomics,
  per-delivery failure policy beyond the engine contract → `plans/rune-autocmds.md`.
- **Job-completion events** — item 7 defines the job substrate; its completions enter as bus events
  and are consumed through this layer unchanged.
- **An engine-owned message log** — outcome detail stays dispatcher-side; the engine merges render
  and foreground status only.
- **Pre-events / veto hooks, event persistence/replay, per-subscriber queues** — bus non-goals,
  unchanged.
- **Pane lifecycle events and non-text buffer kinds** — bus §10; new kinds become subscribable by
  extending the explicit `EventKind` mapping (append-only).
- **Cross-thread subscribers** — dispatch is engine-thread-only, by construction.

## 10. Implementation order

1. **Baseline surface** — session read-only projection + `BaselineContributor` seam + composed,
   capped/fallibly-allocated `BaselineBuilder`; equality-with-`capture_text_lifecycle` tests.
2. **Registration capability** — explicit `EventKind` mapping, lease-carrying subscription entries
   in `RegistrationBatch` (with entry accounting), preflighted contiguous `SubscriptionId`
   reservation, caps; registry-level tests including the ABA case.
3. **Pass-level coordinator** — extend the drain to capture/barrier/snapshot/initialize/deliver,
   with id snapshots, resumable invocation slicing, per-invocation re-resolution, `BoundedStatus`,
   and the shared gate; recording test dispatcher.
4. **Lifecycle + cutoff** — `NeedsBaseline | Live | Disabled`, boundary initialization (including
   empty-queue drains), next-pass eligibility; the double-apply and initializer-fired-event
   regressions.
5. **Barrier recovery + failure policy** — demotion on marker, retry/disable via
   `MAX_BASELINE_ATTEMPTS`; exactly-once resumption tests.
6. **Integration gate** — full drain cycles through the real engine loop with the recording
   dispatcher; run-loop payload routing (thread-local host vs. boxed fallback); calibrate §8.
