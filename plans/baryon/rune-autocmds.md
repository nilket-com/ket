# Specification: Rune Autocmds

The Rune binding over the engine subscriber layer (`plans/event-subscribers.md`, committed) — the
autocmd slice of item 9. It is deliberately **thin**: the engine already owns registration atomicity,
`SubscriptionId` identity, the `NeedsBaseline | Live | Disabled` lifecycle, pass cutoffs, barrier
recovery, and outcome interpretation. This plan defines only what is Rune's: the registration surface,
the two-callback handler ABI, buffer filtering, VM budgets/retained memory, delivery-failure policy,
and the run-loop routing that replaces the current fail-closed arm.

Inherited and not reopened: the subscriber contract (composed baseline, next-pass eligibility,
`EventsLost` demotion, `MAX_BASELINE_ATTEMPTS`), the VM lifecycle (`plans/rune-vm-lifecycle.md`:
per-generation callback tables, `RetainedRuneBudget` signed deltas, `HostBudget` per-entry reset,
deferred lifecycle actions), and the buffer API's value surface (`plans/rune-buffer-api.md`:
`Buffer`/`View`/`Version` opaque handles, guarded-editor ABI).

## 1. Registration — evaluation-time, staged, two distinct callbacks

```rune
baryon::autocmd(editor, [EventKind::BufferEdited, EventKind::BufferClosed],
                initialize, event);
baryon::autocmd_for_buffer(editor, buffer, [EventKind::BufferEdited],
                           initialize, event);
```

- **Two distinct function arguments — `initialize(editor, baseline)` and `event(editor, event)` —
  never one callback with a tagged argument.** Baseline recovery has a dedicated function to land
  in: a registration cannot omit the rebuild callback or accidentally handle recovery as an
  ordinary event. `Function` does not expose a useful registration-time arity proof, so the host
  validates the actual two-argument ABI on first initialization; an arity/type mismatch is an
  initialization failure under the engine's existing retry/disable policy, not a claim that source
  evaluation statically proves the callback body correct.
- **`EventKind` is a typed Rune value** (`#[derive(Any)]`, one constructor per engine
  `EventKind` variant, same names). A registration takes a **kind-set**: a Rune vector of
  `EventKind` values, deduplicated into the engine's `EventKindSet`; an empty vector is rejected at
  the call (mirroring the registry's `EmptySubscriptionKinds`). Before iterating, the native host
  function checks `kinds.len() <= EventKind::COUNT`; a million duplicate values must not turn
  deduplication into uninterruptible host work (Rune's instruction budget does not run inside host
  functions). Duplicates are accepted only within that structural input cap.
- **Registration is evaluation-time only**, exactly like `baryon::command`: it stages into the
  owner's `SourceStaging` batch and publishes atomically with the generation
  (`replace_owner_generation`). Calling it outside source evaluation is the same error commands
  produce. Runtime (mid-callback) registration is deferred with the same rationale.
- Both callbacks are stored in the generation's `Function` table and **covered by the same
  retained-memory accounting as command callbacks**: their captured Rune state is part of the
  generation's measured evaluation allocation, callback growth is settled by signed delta, and
  unload/replacement releases the generation's charge. The bounded host-side `Function` vector is
  structural storage under the existing registration-entry cap; registration does not invent a
  second byte-accounting system. The subscription's `ActionLease` payload is:

  ```rust
  RuneAutocmdRef {
    initialize: RuneCallbackRef,   // reused unit, one per callback
    event: RuneCallbackRef,
    buffer_filter: Option<BufferId>,
  }
  ```

  reusing `RuneCallbackRef` as the callback unit rather than inventing a parallel token.

## 2. Buffer filtering — optional, subject-matched, validated at registration

- **This cut supports `Option<BufferId>` filtering only** (`autocmd_for_buffer` resolves the
  `Buffer` handle at registration). Registration functions follow the existing command/option/map
  ABI (`VmResult<()>`): a stale/non-text filter is a bounded registration failure, not a
  `BufferError` return from the read/edit API. Source evaluation runs on one thread, but the
  validation remains explicit so a retained token cannot silently create a never-matching handler
  if runtime registration is added later.
  Glob/path filtering is deferred with the path-lookup surface (§8).
- **The filter matches the event's subject buffer**: `BufferOpened/Closed/Edited/Replaced` and
  `HydrationCompleted` by their `buffer`; `ViewCreated/Closed` by their `buffer`; `ViewRetargeted`
  matches if **either** `old_buffer` or `new_buffer` equals the filter (both sides of a retarget
  concern a buffer-scoped handler).
- **Buffer-less kinds (`FocusedTextViewChanged`, `SourceLoaded`, `SourceUnloaded`) are rejected at
  registration when combined with a filter** — an unambiguous early error beats silently
  never-matching.
- A filtered-out delivery returns `Delivered` **without entering the VM** — no budget charge, no
  callback invocation; it costs one coordinator step only. It also does **not** reset the binding's
  consecutive-callback-failure counter: absence of an invocation is not a successful callback.
  After the filtered buffer closes, the handler receives its `BufferClosed` (subject matches) and
  then nothing; re-registration is the script's concern.
- The filter does **not** alter the baseline: `initialize` always receives the full composed
  baseline — recovery semantics stay uniform, and the handler filters its own view of it.

## 3. Delivery ABI — typed, invocation-scoped snapshots

Both callbacks follow the settled callback shape: the editor arrives as the guarded `&mut EditorHost`
parameter, never captured. Both return **unit**. A non-unit return is a bounded callback error; Rune
output remains in the bounded `MessageLog` and is not promoted to foreground status on every event.

The event and baseline arguments are **borrowed, invocation-scoped Rune values**. The host constructs
a stack wrapper and passes `&AutocmdEvent` / `&AutocmdBaseline` through `GuardedArgs`; Rune invalidates
an escaped reference when the call guard drops, exactly like the guarded editor argument. Scripts may
copy the small opaque identity tokens returned by accessors, but cannot retain the event record or a
whole baseline. This matters for the baseline: cloning up to 16,384 entries per subscriber or letting
a script retain host-allocated baseline vectors would bypass the retained-memory protocol.

The pending subscriber pass therefore stores its one composed baseline as
`Arc<SubscriberBaseline>`. `AutocmdBaseline` holds only an `Arc` clone for the duration of the call,
and is itself passed by guarded reference with no Rune `Clone` protocol. There is still one baseline
allocation per captured pass, not one clone per subscriber; changing the engine field to `Arc` does
not make the subscriber substrate language-specific.

- **`AutocmdEvent`** (`#[derive(Any)]`, opaque): `kind() -> EventKind`, plus typed accessors that
  return `Option`. The mapping is explicit and exhaustive:

  | Accessor | Event kinds |
  |---|---|
  | `buffer() -> Option<Buffer>` | `BufferOpened/Closed/Edited/Replaced`, `HydrationCompleted`, `ViewCreated/Closed` |
  | `view() -> Option<View>` | `ViewCreated/Retargeted/Closed` |
  | `acting_view() -> Option<View>` | `BufferEdited` |
  | `old_buffer()/new_buffer()` | `ViewRetargeted` |
  | `version() -> Option<Version>` | `BufferOpened` |
  | `old_version()/new_version()` | `BufferEdited/BufferReplaced` |
  | `delta_count()/deleted_bytes()/inserted_bytes()` | `BufferEdited` |
  | `from()/to() -> Option<View>` | `FocusedTextViewChanged` (`None` retains its synthetic-focus meaning) |
  | `source()/owner()` | `SourceLoaded/SourceUnloaded` |

  `version()` is required: without the `BufferOpened.state` carried by the engine event, a consumer
  cannot losslessly fold events into the baseline, and querying the buffer later is unsound if a
  subsequent close was committed before delivery. Accessors return the buffer API's existing handle
  types; an id in an event is not a liveness guarantee (`BufferClosed` hands the script an id whose
  operations return `StaleBuffer`).
- **`Source` and `Owner` are new opaque equality-only Rune tokens** over `SourceId` and
  `RegistryOwnerId`, with `Clone`, `PartialEq`, and `Debug` but no ordering or numeric conversion.
  Returning `u64` would not be equality-only and would expose arithmetic on identity. The source
  accessors above return these types.
- **`AutocmdBaseline`** (`#[derive(Any)]`, opaque):
  `buffers(editor) -> Vec<(Buffer, Version)>`,
  `views(editor) -> Vec<(View, Buffer)>`, `focused() -> Option<View>`, and
  `sources(editor) -> Vec<(Source, Owner)>`. The owner half is mandatory: baseline replacement must
  reconstruct the same source association that `SourceLoaded/SourceUnloaded` events carry. Each
  materializing accessor charges one host call; its bounded Rune `Vec` allocation occurs inside the
  callback allocation limiter. It does not charge read bytes because it copies identities, not
  document content.
- **Documentation contract stated in the API docs**: `initialize` must *replace* the handler's
  model, not merge — it runs on first registration **and** after `EventsLost`, and the handler is not
  given a cause tag (deliberately: both mean "your model is not trustworthy"). The contents may of
  course differ, so this is an API guarantee about the call shape, not a claim that script state can
  never infer that the world changed.

## 4. VM budgets and effects — each delivery is a VM entry

- Every `initialize`/`event` invocation is a **full VM entry**: `HostBudget` three-counter reset,
  instruction budget from the owner generation's `callback_allowance`, `HostEffects` reset/taken and
  merged into the delivery's `SubscriberOutcome.needs_render`, deferred lifecycle actions applied
  after the callback returns (a handler may `source!`/`unload!`; self-unload defers exactly as it
  does for commands — the engine's delivery-time re-resolution then skips the removed
  subscriptions for the rest of the pass).
- **Signed-delta retained accounting on error**, identical to command callbacks: a callback that
  errors after allocating still settles its memory delta.
- Successful callbacks must return `()`. `print`/`println`/`dbg` output stays in `MessageLog` rather
  than turning a frequently-fired autocmd into foreground-status spam. Deferred lifecycle failures
  make the subscriber delivery fail; successful `source`/`unload` acknowledgements are logged but
  do not replace foreground status.
- **Callback-raised events remain next-pass deliveries structurally**: host functions push onto
  `session.events`, and the coordinator's pass was captured before the VM entered. The binding adds
  no mechanism — but adds the real-VM regression test (§7), because this is the property that keeps
  wake chains from re-entering the current pass.
- **Per-entry quotas do not terminate a successful feedback loop.** They bound the work and memory
  of each link; resetting them on every VM entry means a handler that successfully emits one new
  matching event forever can continue forever. Responsiveness remains bounded by
  `MAX_SUBSCRIBER_STEPS_PER_DRAIN` plus the self-wake, but there is no sound causal chain identifier
  with which this thin binding could impose a total-chain cap. This cut records that behavior and
  tests bounded progress; terminal cycle detection is deferred rather than falsely attributed to
  `HostBudget`.

### 4.1 Run-loop borrow split

The current run loop can pass `&RuneHost` as `BaselineContributor` only because subscriber delivery
is fail-closed. Real dispatch needs `&mut RuneHost`, and Rust cannot pass separate immutable and
mutable borrows of that host across one coordinator call.

The run-loop router therefore owns `Option<&mut RuneHost>` and implements **both** existing traits:
`BaselineContributor` delegates through a short shared reborrow, while
`SubscriberDispatcher<EditorHost>` delegates through a later mutable reborrow. The coordinator's
run-loop entry point sequences those borrows: capture/barrier handling and baseline composition
finish first; only then does initialization/delivery borrow the router mutably. No reference to the
baseline contributor survives into callback dispatch. The generic recording-test adapter may keep
the existing separate contributor shape, but production must have one sequential owner rather than
clone the source list on every drain or use unsafe aliasing.

This preserves the composed-ownership decision: the engine still learns only the bounded live
`(SourceId, RegistryOwnerId)` pairs contributed through the read-only trait, never `SourceKey` or
Rune host internals.

## 5. Repeated-delivery-failure policy — binding-side, bounded reporting

The engine never unsubscribes on a `Failed` delivery (correctly — that is policy, not mechanism).
The binding's policy:

- The host tracks **consecutive entered-callback failures per `SubscriptionId`**: VM error,
  instruction/host/allocation-budget exhaustion, return decoding, retained-accounting failure, or
  deferred-lifecycle failure. A successfully entered event callback resets the count.
- Invocation-gate failures are deliberately **not** counted. `InvocationGate` wraps the dispatcher,
  so reentrancy/owner-liveness rejection occurs before `RuneHost::deliver` and cannot honestly be
  attributed to a callback attempt without changing the language-neutral coordinator contract.
  The coordinator reports that isolated failure as it does today. Likewise, filtered-out events and
  baseline initialization success/failure do not change the event-callback counter.
- After **`MAX_AUTOCMD_CONSECUTIVE_FAILURES`** (initial: 8) the binding **mutes the handler**: it
  stops invoking the event callback (returns `Delivered` without a VM entry) until its **owner
  generation reloads** — a reload replaces the subscription wholesale and starts fresh. Muting is
  binding-side state; the engine's subscription stays `Live` (the engine contract is untouched).
- Failure state is stored owner-scoped in `RuneHost`, keyed by the never-reused `SubscriptionId`.
  Publication starts new IDs at zero failures; successful owner replacement and unload remove all
  counters for the retired owner. Failed publication does not disturb the old generation's state.
  This cleanup is required—monotonic IDs make stale map entries safe from ABA, not bounded in
  memory. A muted subscription still runs `initialize` after an overflow barrier because the engine
  cannot mark a baseline trustworthy without a successful rebuild; muting suppresses ordinary
  event delivery only and does not reset on that rebuild.
- Counter settlement occurs **after** the VM result, retained-memory delta, and deferred lifecycle
  actions are known, but updates the map only if the invocation's owner/subscription is still live.
  A callback that unloads its own source may still return its current outcome, but cleanup wins: the
  result must not recreate failure state for the retired owner.
- **Bounded reporting**: the first failure and the muting transition produce a foreground status +
  message-log entry; intermediate failures log to the `MessageLog` only (already line- and
  size-bounded). No per-failure foreground spam, no unbounded accumulation.
- `initialize` failure needs no binding policy — the engine already owns it
  (`MAX_BASELINE_ATTEMPTS` → `Disabled`).

## 6. Run-loop routing — replace the fail-closed arm

- `RuneHost` implements `SubscriberDispatcher<EditorHost>`: `initialize` resolves
  `RuneAutocmdRef.initialize` against the owner generation's callback table and invokes with the
  `AutocmdBaseline` value; `deliver` applies the buffer filter, then resolves and invokes
  `RuneAutocmdRef.event` with the `AutocmdEvent` value. Owner-liveness uses the host's existing
  generation table (same semantics as `DynamicActionExecutor::owner_is_live`). Before either
  lookup, both nested `RuneCallbackRef.owner` values must equal the outer
  `SubscriberInvocation.owner`; a mismatch is a bounded capability error, never a cross-generation
  callback lookup. Staging makes the invariant true, while the dispatcher check makes it
  structural against malformed internal/test payloads.
- `RunLoopSubscriberDispatcher` routes a lease whose payload is `RuneAutocmdRef` to the
  thread-local mutable `RuneHost`; it also supplies that host's read-only source contribution via
  the sequential borrow rule in §4.1. **Every other payload keeps flowing to the boxed fallback dispatcher** —
  the existing `rune_typed_subscriber_never_falls_through_to_the_boxed_dispatcher` test flips into
  its positive form (Rune payload → host) while a companion test pins that non-Rune payloads still
  reach the boxed fallback unchanged.
- The interim "Rune event subscriber binding is unavailable" failure arm is deleted; a
  `RuneAutocmdRef` payload with a missing generation/callback (possible only across a bug, since
  removal is atomic with the subscription) fails with a bounded status, not a panic.
- The router does not classify failures or own mute state. Once it selects the typed payload,
  `RuneHost` owns filtering, callback entry, signed-delta settlement, failure counting, logging, and
  outcome construction; a Rune failure never falls through to the boxed dispatcher.

## 7. Testing (real VM, through the engine loop)

- **Initial baseline**: an `init.rn` autocmd receives exactly one `initialize` with the startup
  buffer/view/focus projection before any event; no retrospective transition events.
- **Typed accessors**: an edit delivers an `AutocmdEvent` whose `kind`, `buffer`, `acting_view`,
  `old_version != new_version`, and byte counts match the committed edit; a retarget exposes both
  buffers; `BufferOpened.version` matches the event's committed state even if the buffer is closed
  before delivery; focus events expose `from`/`to` with `None` for synthetic; source events and the
  recovery baseline expose the same opaque `(Source, Owner)` association.
- **Guarded argument lifetime**: retaining either callback's `editor`, `AutocmdEvent`, or
  `AutocmdBaseline` through direct return and nested-container paths produces a clean later
  `VmError` in child-process tests; copied `Buffer`/`View`/`Version`/`Source`/`Owner` tokens remain
  ordinary opaque values. A many-subscriber baseline test proves one shared baseline allocation,
  not one vector clone per initialization.
- **`EventsLost` rebuild**: force an overflow (test-sized queue); the handler's `initialize` runs a
  second time with the post-overflow state, ordinary deliveries resume exactly once afterward, and
  the handler observed no marker-shaped event.
- **Owner reload/removal**: `:source` reload swaps the handler atomically — old generation's
  callbacks never fire after publication, new generation initializes from current state;
  `unload!` from *within* a delivery defers, then the remaining pass skips the removed
  subscriptions.
- **Buffer filtering**: a two-buffer session with a filtered handler sees only its buffer's edits,
  sees the retarget from either side, and a filtered registration containing `SourceLoaded` is
  rejected at evaluation with a clear diagnostic.
- **Failure isolation and policy**: a handler that always errors mutes after
  `MAX_AUTOCMD_CONSECUTIVE_FAILURES` with first-failure + muting statuses only, while a sibling
  handler keeps receiving every event; a success mid-stream resets the counter. Filter misses,
  baseline rebuilds, and an invocation-gate rejection do not reset or increment the binding-owned
  counter; reload/unload reclaim its owner-scoped entries.
- **Callback-raised events**: a handler that edits the buffer on `BufferOpened` sees its own
  `BufferEdited` arrive in a later pass (never the current one), and the chain terminates when the
  handler stops firing. A deliberately self-triggering successful handler characterizes the real
  contract: each drain slice stays bounded and schedules a wake, but no total-chain cutoff is
  claimed.
- **Retained-memory deltas**: callbacks capture allocated Rune state, callback execution grows and
  releases it, reload replaces the generation charge, and unload releases it exactly (assert the
  budget's charged value round-trips to its pre-registration level). Do not infer a separate byte
  charge merely from placing two host `Function` handles in the bounded callback vector.
- **Non-Rune fallback**: a boxed-dispatcher subscription (test payload) still initializes and
  delivers with the Rune host present and Rune autocmds registered.

## 8. Non-goals / deferred

- **Glob/path/pattern filtering** — needs the path-lookup surface; deferred with it.
- **Runtime (mid-callback) autocmd registration** — same deferral as runtime command registration.
- **Pane/option/keymap/command event kinds** — arrive automatically when the bus grows kinds
  (append-only `EventKind`).
- **Once-only autocmds, groups, ordering control between handlers** — sugar over the engine's
  deterministic `SubscriptionId` order; deferred until demanded.
- **Unmute short of reload** — muting ends at generation replacement only, keeping the state
  machine two-valued.
- **Successful autocmd-cycle detection** — per-entry budgets and drain slicing preserve
  responsiveness but do not establish causality across passes; a sound total-chain policy needs a
  separate design.

## 9. Constants (initial; pinned at the integration gate)

| Limit | Value | Basis |
|---|---|---|
| `MAX_AUTOCMD_CONSECUTIVE_FAILURES` | 8 | exact boundary characterized: tolerate transient errors; mute a persistently broken handler |
| kind-set input length per registration | the engine's `EventKind` count | checked before native iteration, then deduplicated |

## 10. Implementation order

1. **Payload + staging** — `RuneAutocmdRef`, `EventKind`/kind-set Rune values,
   `baryon::autocmd`/`autocmd_for_buffer` staging into `SourceStaging` with retained accounting;
   registration-validation tests (empty kinds, input length above `EventKind::COUNT` rejected before
   iteration, duplicate kinds within the cap, filter × buffer-less kinds, stale filter handle).
2. **Guarded value surface** — make the pending pass baseline `Arc`-shared; add
   `AutocmdEvent`/`AutocmdBaseline` plus opaque `Source`/`Owner`, exhaustive typed accessors, and
   escaped-borrow tests.
3. **Dispatcher** — `SubscriberDispatcher<EditorHost>` on `RuneHost` (unit-return VM entry,
   budgets, effects, deferred lifecycle), buffer filter short-circuit.
4. **Sequential routing** — let the run-loop router contribute sources through a shared reborrow and
   dispatch through a later mutable reborrow; replace the fail-closed arm; positive-routing +
   boxed-fallback tests.
5. **Failure policy** — owner-scoped consecutive-failure tracking, cleanup, muting, and bounded
   reporting with filter/baseline/gate non-effects pinned.
6. **Integration gate** — the §7 real-VM suite through the engine loop on both resolver backends;
   calibrate §9.
