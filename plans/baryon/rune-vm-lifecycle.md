# Specification: Rune VM Lifecycle

Issue-inventory item 8 of `plans/rune-scripting.md`. This spec turns the settled decisions of
`rune-scripting.md` §2 and the spike findings of `rune-host-access-spike.md` into a concrete
lifecycle for a Rune virtual machine embedded on the engine thread: how the runtime is constructed
across the `Send` thread boundary, how `init.rn` is compiled and executed, how the guarded
`EditorHost` facade is passed in, how script registrations reach the runtime-registry overlay, how
output and diagnostics surface within fixed bounds, how script execution and retained memory are
bounded, and how a script's registrations are owned, source-indexed, and atomically replaced on
reload.

It deliberately **excludes** the event bus (`rune-scripting.md` item 6) and the `baryon::*` buffer/
cursor API (item 9). The runtime-registry overlay (`plans/runtime-registry-overlay.md`) is complete
and provides the ownership, action, and lifecycle machinery this spec drives; the Rune host is the
first real `DynamicActionExecutor`, replacing `UnavailableActionExecutor`.

**Implementation status:** delivered through the startup, registration/callback, and live-source
cuts. Section 9 preserves the cut boundary as implementation history; its originally deferred
follow-ons are now complete.

## 1. What already exists, and the constraints it imposes

- **The executor seam is `Send` and type-erased.** `Engine.dynamic_action_executor:
  Box<dyn DynamicActionExecutor<EditorSession, Output = DynamicActionOutcome> + Send>`
  (`engine/core.rs:229`); `with_dynamic_action_executor` requires `Send + 'static` (`core.rs:292`).
  The trait exposes **`invoke` only**. `InvocationGate::invoke_dynamic<C, E>` (`runtime_registry.rs:1568`)
  is **generic over the context `C`**, so two executors with different contexts share one gate.
- **The engine crosses a thread boundary.** `Engine` is moved into `thread::spawn(move || engine.run())`
  (`host.rs:117`), so **everything reachable from `Engine` must be `Send`.** `rune::Vm` and stored
  `rune::Function` values are `!Send` — **a Rune runtime cannot live on `Engine`.**
- **`rune::Context` and `rune::Unit` are statically `Send + Sync`** (`rune-0.14.1/src/compile/context.rs:1252`,
  `runtime/unit.rs:51`), so reading and compiling a source can run on a worker thread and the
  resulting `Arc<Unit>` can cross to the engine thread. Only the `Vm`/`Function` execution is `!Send`.
- **Rune functions are fixed-arity.** `Function::call` enforces an exact argument count
  (`runtime/function.rs:864`). One registered action = one callback of one fixed arity.
- **Rune's allocation limiter is per-call transient.** `rune-alloc` restores the previous
  thread-local limit after each call (`rune-alloc-0.14.1/src/limit/mod.rs:151`): a per-call cap bounds
  one call, **not** cumulative retained memory across calls, Units, and captured state — and the
  measurement is unrecoverable once the guard exits.
- **Ownership/lifecycle baseline (Stage 6).** Monotonic `RegistryOwnerId`s/revisions, bounded atomic batches,
  precedence-preserving `replace_owner_generation`, `remove_owner`. **Owner IDs are strictly
  monotonic and never reused** (`allocate_owner`, `runtime_registry.rs:624`). **`remove_owner` bumps
  the revision and can fail.** Before this lifecycle follow-on, `OwnerState` had no publication flag;
  §7 adds and publishes that state explicitly.
- **Initial file load is already separate from `EditorSession::new`.** `run_engine` constructs the
  session, wires input, then `load_file_into_pane(...)` opens the initial file (`runtime.rs:2079`)
  before the receive loop. Inserting `run_init` between those two points needs **no extraction
  refactor**.
- **Resolution & routing.** Resolution yields `InvocationSnapshot { revision, owner, invocation }`;
  `action::<T>()` (`runtime_registry.rs:1454`) downcasts the owner-tagged `ActionLease` payload.
  `DynamicInvocation` is `Command { canonical_name, spelling, args, bang } |
  Option { canonical_name, operation } | Mapping { mode, sequence }`. The gate enforces
  non-reentrancy, owner liveness, and the 4 KiB option-query display bound; a callback return becomes
  `DynamicActionOutcome { status_message, option_query_display, needs_render }`. A resolved lease
  survives same-owner replacement (Stage 3).
- **Rune the crate.** `rune 0.14.1`, `default-features = false, features = ["std"]` — already linked.
  The default thread-worker path needs no new dependency/feature; `emit` (codespan) stays off. The
  conditional process-isolation branch may need a small direct serialization-codec dependency, but
  no additional Rune feature (`Unit` already has serde support under `std`).

## 2. Threading, construction, and payload-based routing

`Engine` cannot hold the runtime; split `Send` config from the `!Send` runtime:

- Add `Engine.rune_host: Option<RuneHostConfig>` — **`Send`**: the resolved `init.rn` path (or
  `None`), the §8 constants, and the embedder opt-in. This crosses `thread::spawn`.
- Inside `run_engine`, construct the **non-`Send` `RuneHost` as a local** from `RuneHostConfig`.

**Route dynamic invocations by payload, not by whether a Rune host exists** — a Rune host may coexist
with boxed-executor-owned actions:

```
if snapshot.action::<RuneCallbackRef>().is_some() {
    gate.invoke_dynamic(rune_host.as_mut().ok_or(HostUnavailable)?, &mut editor_host, snapshot)
} else {
    gate.invoke_dynamic(&mut *boxed, &mut editor_host.session, snapshot)
}
```

- Rune payload → `RuneHost`; other payload → the boxed `Send` executor. A Rune payload with no host
  is a checked `HostUnavailable` action error, and **a Rune failure never falls through to the boxed
  executor.** Both share the one `C`-generic `InvocationGate`. The boxed executor keeps
  `&mut EditorSession` (via `EditorHost`'s `DerefMut`); only Rune needs `&mut EditorHost`.
- `RuneHost` implements `DynamicActionExecutor<EditorHost>` for `invoke` **and** carries inherent
  lifecycle methods (`run_init`, `source`, `unload`) that the invoke-only trait cannot express.

## 3. The ownership graph

```rust
#[derive(rune::Any)]
#[rune(item = ::baryon, name = Editor)]
struct EditorHost {
    session: EditorSession,              // real state; private. Deref/DerefMut -> EditorSession.
    services: Arc<Mutex<HostServices>>,  // shared with the std::io shims (§5)
    staging: Option<SourceStaging>,      // Some only while a source is being evaluated
}
struct RuneHost {
    context: rune::runtime::RuntimeContext,             // ONE persistent runtime context
    services: Arc<Mutex<HostServices>>,
    generations: BTreeMap<RegistryOwnerId, Generation>, // liveness = keys of this map
    sources: BTreeMap<SourceKey, SourceSlot>,           // source identity index (below)
    next_source_request_id: SourceRequestId,            // host-global monotonic, never reused (§7)
    pending_compile: Option<PendingCompile>,            // at most one reserved compiler job (§8)
    retained: RetainedRuneBudget,                       // session-wide memory accounting (§8)
}
struct Generation { unit: Arc<rune::Unit>, callbacks: Vec<rune::Function>, retained_bytes: usize }

enum SourceKey { Init, Path(CanonicalPath) }
// SourceRequestId is an impl_id_unit! newtype like RegistryOwnerId: strictly monotonic,
// never reset, never reused, allocation checked at exhaustion.
struct SourceSlot { current_owner: Option<RegistryOwnerId>, latest_request: SourceRequestId }
struct PendingCompile { source: SourceKey, request: SourceRequestId, reserved_bytes: usize }
struct HostServices { messages: MessageLog, budget: HostBudget }
```

- `EditorHost` owns the session and replaces it as `run_engine`'s stack local (Option A, §9);
  `Deref`/`DerefMut` is a derived accessor, not parallel state.
- `RuneHost` owns the runtime: one `RuntimeContext`, and per generation (keyed by `RegistryOwnerId`) a
  `Unit`, its `Function` callbacks, and its charged `retained_bytes`.
- **Owner liveness is derived from `generations`** — no second live-owner set.
- **`sources` gives each loadable source a stable identity** so reload, unload, and stale-result
  rejection are per-source, not global (§7). Live and pending slots, their path bytes, and the
  compile scheduler are independently bounded (§8); unloaded slots are removed rather than retained
  as tombstones.

### The registration bridge — `SourceStaging`

Native registration functions receive `&mut EditorHost`, never `&mut RuneHost`. Bridge with a staging
buffer on `EditorHost` for one source evaluation:

1. `RuneHost` sets `host.staging = Some(SourceStaging::new(owner))` before evaluating.
2. `baryon::command(name, spec, callback)` (+ option/mapping analogues) push metadata + the
   `rune::Function` into staging. **Each push enforces the overlay's caps incrementally** (per-field
   bytes, entry count, metadata bytes, callback count — the same constants `commit_owner_batch`
   re-checks) and **rejects before copying**, so a script cannot stage 100 000 registrations and force
   huge Rust-side cloning before the transaction caps reject it.
3. After the guarded call returns, `RuneHost` takes staging, assigns `callback_id`s, builds **one**
   `RegistrationBatch` with `ActionLease` payload `RuneCallbackRef { owner, callback_id }`, and
   publishes (§7).
4. Success → insert/replace the `Generation`. Any failure → drop staged callbacks + batch and discard
   the candidate owner (§7).

Registration is source-evaluation scoped in this lifecycle: calling a registration function while
`host.staging` is `None` returns a clean Rune error and changes nothing. Supporting ad-hoc catalog
transactions from an ordinary callback would need its own explicit transaction API; it must not be
accidentally enabled by unwrapping a missing staging buffer.

## 4. Callback/Unit/Function model and the invocation ABI

Invoke the stored `Function` directly (it carries its own context+`Unit`); never `vm.call(entry, …)`
across generations.

- **`init.rn` entrypoint:** `pub fn main(editor)`. `RuneHost::run_init` calls `main` with `(&mut host,)`
  under the startup budget; return ignored beyond error reporting.
- **Callback invocation** (`invoke`, context `&mut EditorHost`): resolve `Generation`/`callback_id`
  from `RuneCallbackRef`, then `callback.call(args)`, **fixed arity per kind**:
  - `Command { spelling, args, bang, .. }` → `(editor, args: String, bang: bool)`.
  - `Option { operation, .. }` → **`(editor, call: OptionCall)`** — one callback, one arity. `OptionCall`
    is a Rune enum `Query | SetBool(bool) | SetInteger(i64) | SetString(String)` from
    `OptionInvocation`/`OptionValue`; for `Query` the returned string → `option_query_display` (4 KiB).
  - `Mapping { .. }` → `(editor)`.
- **Return → `DynamicActionOutcome`:** unit → default; `String` → `status_message` (≤ `MAX_STATUS_BYTES`,
  §8) except option `Query` → `option_query_display`. `VmError`/exhaustion → `ActionError`.

Implemented in the registration cut (§9); the first cut needs only `pub fn main(editor)`.

## 5. Output capture

- **The `print!`/`println!`/`dbg!` macros survive `with_config(false)`** — the flag drops rune's `io`
  *functions*, but the macros still expand to `::std::io::{print, println, dbg}` (refines
  `rune-scripting.md` §2.7). **Install bounded functions in a replacement `::std::io` module**; they
  do not receive `EditorHost`, so they capture the shared `Arc<Mutex<HostServices>>` and write to a
  bounded `MessageLog`. Ordinary syntax works; the ratatui screen is never touched.
- `MessageLog` is ring-bounded (§8); oversized lines truncate with an elision marker. After each
  top-level call, `RuneHost` may project the newest message **emitted by that call** into
  `status_message` without removing it from the log. Record the log sequence before invocation so a
  silent callback cannot resurface an old message. A bounded compile/runtime error wins over an
  explicit callback return, which wins over printed output; otherwise the newest emitted line is
  projected.
- Shims are total: never panic (`panic = "abort"`), never allocate unboundedly, recover a poisoned
  services lock without `unwrap`, and are lossy on invalid UTF-8. Formatting stops at the line cap;
  it does not first build an unbounded intermediate string and truncate afterward.

## 6. Diagnostics, bounded

Render from structured `rune::Diagnostics` (never `emit`/codespan). Cap diagnostic **count**,
**per-message bytes**, and **total bytes** (§8). The compile worker returns this bounded summary
alongside the `Arc<Unit>`. Runtime `VmError`s render under the same bounds. Append rendered entries
to `MessageLog`; the status line is only the bounded foreground projection, not the sole copy.

## 7. Startup / reload / atomic source-generation replacement

- **Opt-in & path.** `EngineConfig.init_script: Option<PathBuf>`, default `None`; the library host
  runs nothing unless set. The TUI derives its path from `EngineSettings.settings_path`. Absent file
  ⇒ no init, no diagnostics.
- **Ordering — `init.rn` runs BEFORE the initial file loads.** Insert `run_init` between session
  construction (+ input wiring) and `load_file_into_pane` (`runtime.rs:2079`); no extraction needed.
  Configuration must register buffer-open hooks before the CLI file opens; a `VimEnter`-like event
  fires *after* the initial load once the bus exists. **This order is locked** — changing it later is
  a compatibility break. `run_init` retains a bounded startup report until after the initial load so
  that load-time status changes cannot erase the only visible script diagnostic before the first
  render. A file-load error wins, then an init error, init output, and routine load status; every
  script diagnostic/output line also remains in `MessageLog`.
- **Compile failure ⇒ editor still starts** in settings-only state with bounded diagnostics and empty
  `generations`. A broken config never bricks the editor.

### Source identity, reload, and stale-result rejection

Every load targets a `SourceKey`, keeping reload/unload/stale-rejection **per source, not global**.
**`SourceKey` is normalized:** the configured `init.rn` path resolves to `SourceKey::Init`, so a later
`:source /path/to/init.rn` on the same physical file maps back to `Init` rather than minting a second
`Path` owner (which would double-register and collide in the namespace). Otherwise `Path(canonical)`.

- **Load/reload a `SourceKey`:** preflight the source-slot/path-byte limits, then allocate a fresh
  **host-global `SourceRequestId`**
  (`next_source_request_id`, monotonic, never reset/reused, exhaustion-checked like `RegistryOwnerId`),
  store it in `sources[key].latest_request`, and dispatch the off-thread read+compile (below) tagged
  with that id. ID exhaustion or scheduler admission failure leaves the slot and queue untouched.
  Startup `init.rn` uses the same ID allocation even though it compiles inline, so every extant slot
  has a real latest request. `:source a.rn` then `:source b.rn` are distinct keys and both load; a repeated
  `:source a.rn` supersedes only earlier `a.rn` requests. A **global** id (not a per-slot counter)
  closes the ABA hole where unloading a source and re-requesting it would restart a per-slot counter
  at 1 and wrongly accept a stale in-flight result.
- **On the worker result:** accept only if the result's `SourceRequestId == sources[key].latest_request`
  (per-key latest-wins; a result for a since-removed slot is rejected). Then allocate a candidate
  owner, evaluate on the engine thread (staging → batch), and publish:
  - `sources[key].current_owner == None` → `commit_owner_batch` (first load of this source).
  - `Some(current)` → `replace_owner_generation(current, batch)` (atomic, precedence-inherited).
  On success, set `sources[key].current_owner = candidate`, insert the `Generation`, and reclaim the
  retired generation's charge (§8).
- **Failure retains the last good generation.** Read, compile, evaluation, admission, or publication
  failure never clears `current_owner`; it reports bounded diagnostics and leaves the effective
  catalog unchanged. If the failed request created a slot with no current owner, remove that empty
  slot after releasing its request/reservation. A failed reload keeps its slot and last good owner,
  with the failed request ID still preventing an older result from becoming current.
- **Startup publication:** once the registration cut exists, successful startup evaluation publishes
  under `SourceKey::Init` and records its owner in that slot. A later `:source` of the configured init
  path therefore replaces that generation rather than creating a second one.
- **Unload a `SourceKey`:** if it has a current owner, call `remove_owner(current)` and **only on
  success** drop the `Generation` (reclaim charge) and remove the slot. Removing a slot with only a
  pending request invalidates the result; the pending compile reservation remains attached to its
  globally unique request ID until the worker acknowledges cancellation or returns and is discarded.
- **Atomicity is scoped.** Only catalog/callback publication is atomic; output and (later) editor
  mutations performed while *evaluating* a candidate are non-transactional and remain applied
  (`rune-scripting.md` §3.12).

### Source compilation transport

The default path uses the fact that `Context`/`Unit` are `Send + Sync` (§1): **`:source` reads and
compiles on an internal bounded thread worker** (modeled on the path-completion worker:
request-generation stale rejection, latest-wins per `SourceKey`), returning
`{ unit: Arc<Unit>, retained_bytes, diagnostics_summary }`
through the internal event stream; **evaluation runs on the engine thread**. **No on-thread compile
fallback.** The plugin-facing job API stays out of scope. Startup `init.rn` compiles inline before the
loop (one-time pre-interactive cost). Construct one build-time `Context`, derive the persistent
`RuntimeContext` from it, use it for startup compilation, then move that same `Context` to the worker
when live sourcing lands; do not independently rebuild two module catalogs that could drift.

The scheduler is bounded by the source limits in §8 and retains at most one queued request per
`SourceKey`: a newer request replaces that key's queued work, while distinct keys remain ordered.
After producing a compiled `Unit`, the worker waits for the consume/discard acknowledgement before
starting the next compile (§8). Shutdown closes both queues and requests cancellation between worker
phases; it must not block the engine thread indefinitely joining a native compile that cannot be
preempted. An in-progress worker owns no `EditorSession`/`EditorHost`, and a failed result send ends
it; if the latency spike requires a hard termination guarantee, that is another reason to use the
process-isolation branch in §8. If that branch is selected, it replaces this thread transport for
both startup and live source compilation while preserving the same source ordering and publication
contract.

### Publication tracking (overlay follow-on)

Emptiness cannot prove "never published": a published owner may later publish an *empty* batch while
old resolved capabilities survive; removing it would change liveness. **Add `OwnerState.published:
bool`** — `false` at `allocate_owner`, set `true` by every successful publication **including an empty
batch** (including `commit_owner_batch`'s empty-owner early return, revision-neutrally). **Add
`discard_unpublished_empty_owner(owner)`** accepting **only** `!published && entries.is_empty()`,
returning a checked error otherwise, consuming **no revision**. Owner IDs are never reused, so an
undiscarded candidate lingers forever — therefore **every failure after candidate allocation,
including VM-evaluation failure, must discard it.**

### Self-modification during a callback

A callback may reload/unload its own owner; the registry ops never touch the gate (Stage 6
`action_can_remove_its_owner_without_invalidating_the_running_snapshot`), so the running snapshot
completes. `RuneHost` must **defer dropping the currently-executing `Generation`'s callbacks until the
outer `invoke` returns** — a `Function` cannot be freed while on the stack. The delivered
`baryon::source(editor, path)` / `baryon::unload(editor, path)` host calls append to a strictly
bounded callback-local lifecycle queue; `RuneHost` drains it only after `Function::call` and
retained-memory sampling finish.

## 8. Resource limits — an enforceable session-wide protocol

Rune's per-call limiter is transient and its measurement is unrecoverable after the guard exits (§1),
so cumulative memory must be **host-accounted** with an explicit transaction algebra
(`RetainedRuneBudget { session_cap, replacement_headroom: H, charged, reserved }`, `charged` = Σ
published live-generation charges; `reserved` = the memory currently admitted for the one
compiler/candidate transaction). For one reload: `C` = current `charged`, `O` = retired generation
charge, `U` = candidate Unit compile charge, `E` = candidate evaluation-retained charge.

This budget covers **script-attributable Rune allocations** (Units, VMs, returned Rune values, and
captured callback state). Construct the fixed build/runtime contexts in a separate limiter scope
using `compile_allocation_cap`, measure them as host baseline, and fail the optional Rune host cleanly
if construction exceeds it. Rust-owned host metadata is bounded separately (source slots/path bytes,
staging metadata, logs, and diagnostics). Neither category is silently charged as if it belonged to
an arbitrary generation.

- **Thread-worker reserve before compiling.** The engine admits at most one compiler job. Before
  dispatch it computes
  `R = min(compile_allocation_cap, session_cap + H − charged − reserved)` with checked arithmetic,
  stores `R` in `PendingCompile`, adds it to `reserved`, and gives `R` to the worker as its allocation
  limit. While compilation runs, callback admission also observes `reserved`, so live callbacks cannot
  grow into memory promised to the worker. A queued source path has no Unit and needs no memory
  reservation until the engine/worker rendezvous starts its compile.
- **Compile (worker thread):** inside that `R`-sized limiter scope, **after dropping temporary compiler
  state**, measure the retained `Unit` footprint (`U`) and return it with the `Arc<Unit>`. When the
  engine consumes that result, it atomically shrinks the request's reservation from `R` to `U`; on
  compile failure/cancellation it releases `R`. The worker cannot mutate engine-owned accounting,
  and the engine cannot reconstruct `U` after the worker guard exits.
- **One pending Unit, one reservation.** The worker does not begin another compile until the engine
  consumes or discards the current result (single-result rendezvous/acknowledgement). A compiled Unit
  in transit is therefore covered by `reserved = U`; multiple 256 MiB Units cannot accumulate in the
  shared internal-event queue.
- **Per-call limiter = `min(operation_allocation_cap, steady_free, peak_free)`**, all with checked
  arithmetic. For an ordinary callback, `steady_free = session_cap − charged` and
  `peak_free = session_cap + H − charged − reserved`. For candidate evaluation, its Unit is already
  the reservation, so `steady_free` is not an admission promise; use
  `min(evaluation_allocation_cap, session_cap + H − charged − U)`, then apply both publication
  inequalities below. This preserves the distinct per-operation cap while respecting both steady and
  in-flight physical memory.
- **Return values are decoded before sampling.** Return a Rune value, **preflight its length against
  `MAX_STATUS_BYTES` in Rune space, then convert to `String` inside the accounting scope**, and drop
  temporaries **before** reading the limiter. Concretely, do not use
  `Function::call::<std::string::String>`: obtain the Rune value/string, inspect it, and only then
  convert. A direct Rust-string conversion can allocate past the cap before it is checked.
- **Callback deltas are signed and non-transactional.** A callback may free previously retained state;
  if `A` was the installed allowance and `remaining_after` is sampled after return decoding and
  temporary/error drops, `delta = A − remaining_after` is signed. Apply `delta`, with checked arithmetic,
  to **both** the owning `Generation.retained_bytes` and global `charged` **before leaving the limiter
  scope — even when the callback returns `VmError`** (callback state is not rolled back). The limiter
  bounds positive growth; negative deltas reclaim memory.
- **Reload admission — both inequalities must pass before registry publication:**
  - peak (old + candidate coexist): `C + U + E ≤ session_cap + H`
  - steady (after retiring `O`):     `C − O + U + E ≤ session_cap`

  During evaluation the reservation grows from `U` to `U + E`. On success publish the registry and
  generation, set `Generation.retained_bytes = U + E`, set
  `charged = C − O + U + E`, clear the reservation, then drop the retired generation. On any
  evaluation/admission/registry failure, drop the candidate and release its reservation while leaving
  `C` unchanged. A first load has `O = 0`; the same checks apply.
- **Per-generation ceiling is defense in depth, not a substitute for measurement.** If enabled, a
  `MAX_RETAINED_PER_GENERATION` cap participates in callback/evaluation admission and is updated by
  the same signed delta; `MAX_SOURCE_SLOTS × MAX_RETAINED_PER_GENERATION` is then a second concrete
  ceiling. If the spike cannot measure signed retained deltas reliably, **there is no assumed
  Unit-count fallback** — the registration cut remains blocked.

**Mandatory pre-registration spike (falsify-first).** The algebra above is the **candidate protocol**;
a throwaway spike **before the registration cut** must falsify it, exercising: repeated-callback
retained growth (signed deltas, including a callback that frees), multi-source load, reload-at-cap
(both inequalities, headroom behavior), compile reservation while another callback grows, pending-Unit
rendezvous, return-value conversion before sampling, and cross-thread `Unit` drop/reclaim.
The instruction/allocation constants were provisional until this spike and release-mode latency
numbers landed; the completed spike was the **hard gate** on the registration cut.

**Compiler CPU is not preemptible.** Rune's instruction budget meters VM execution, not the native
compiler. The source-byte/allocation caps and single worker bound its input, memory, and concurrency
and keep the editor thread responsive, but they are not a hard wall-clock cutoff. The spike must
measure worst-case release-mode compilation at `MAX_SOURCE_BYTES`; if that is unacceptable, live
`:source` and startup compilation need the hard-isolation branch below rather than a watchdog that
can report a stuck thread but cannot stop it safely.

### Conditional hard-isolation branch

If the latency spike rejects an in-process compiler thread, run each compile in a same-version helper
process and enforce both accumulated CPU time and parent-observed wall time:

- **Linux/other POSIX:** set a child `RLIMIT_CPU` soft+hard limit and have the parent kill on its
  tighter wall-clock deadline. On Linux, an optional delegated cgroup-v2 scope adds `cpu.max`
  bandwidth throttling and `memory.max`; a transient systemd unit's `CPUQuota=`/`MemoryMax=`/
  `RuntimeMaxSec=` are a convenient adapter, not a required desktop dependency. A threaded cgroup can
  throttle the existing worker thread's CPU on Linux, but cannot safely terminate it and does not
  provide per-thread memory control, so it is not the hard-isolation design.
- **macOS:** the child uses `RLIMIT_CPU` plus the same parent wall-clock kill. Keep Baryon's allocator
  and framed-output caps; Darwin's other resource limits are not a substitute for measuring the
  retained Unit in the parent.
- **Windows:** put the child in a Job Object with process/job user-time and memory limits plus
  `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`; the parent deadline terminates the job.

`rune::Unit` derives `Serialize`/`Deserialize` under the enabled `std` feature, so the candidate wire
protocol is a versioned, length-prefixed, **size-capped** serialized Unit plus bounded diagnostics.
The parent checks the frame length before allocation, deserializes inside its `R` reservation, drops
decode temporaries, and measures `U` locally; no child-reported retained size is trusted. The protocol
also fingerprints the module catalog so bytecode compiled against a different host ABI is rejected.
Cancellation/timeout must kill or reap the child, release the reservation, and leave the last good
generation current. This serialization/deserialization path is part of the mandatory spike if hard
isolation is selected; it is not added speculatively to the first cut. In that branch these child-
compile/parent-decode rules replace §8's thread-worker `R`/`U` measurement steps; the peak/steady
publication algebra resumes once the parent has measured `U`.

Concrete initial constants (measured by the spike; tunable without changing the protocol):

| Limit | Value |
|---|---|
| Max `init.rn`/source bytes | 4 MiB; read `limit + 1`, reject before compile; **strict UTF-8** |
| Source identities/scheduler metadata | `MAX_SOURCE_SLOTS = 4,096` live or pending keys / 32 KiB per canonical key / 1 MiB total key bytes |
| Compile-time allocation limit | 256 MiB |
| Session retained-Rune-memory cap + replacement headroom | 512 MiB + 256 MiB replacement headroom |
| Startup / per-callback instruction budget | 50_000_000 / 10_000_000 |
| Startup / per-callback allocation limit | 256 MiB / 64 MiB |
| Host-call quota (init + source eval + callback) | 100_000 native calls |
| One `print`/`println` native call | 64 KiB input; reject before scanning |
| One `dbg!` native call | 64 arguments; each formatter stops at the 4 KiB line cap |
| Deferred callback source lifecycle | 64 actions / 32 KiB per path / 64 KiB total path bytes |
| Returned status string (`MAX_STATUS_BYTES`) | 4 KiB |
| MessageLog | 1_024 lines / 256 KiB; line truncated at 4 KiB + elision |
| Diagnostics | ≤ 64 / 512 bytes per message / 32 KiB total |

The host budget is **load-bearing** (a host function cannot be interrupted — the `budget::acquire()`
defect, `rune-scripting.md` §2.9 / spike §3). Per-operation hard caps (buffer reads) belong to item 9.

## 9. Implementation sequence

The first cut scoped itself to **construct + run `init.rn` + output + diagnostics**:

1. `EditorHost` facade (`#[derive(Any)]`, `name = Editor`) owning `EditorSession`, `Deref`/`DerefMut`,
   `services`, `staging` (unused this cut). Swap `run_engine`'s stack local to `EditorHost` (Option A).
2. `Engine.rune_host: Option<RuneHostConfig>` (`Send`); build the `RuneHost` local when configured.
   Dispatch stays payload-routed (§2); nothing registers a Rune action this cut, so the boxed executor
   stays `UnavailableActionExecutor` and its path is unreachable.
3. Runtime: `Context::with_config(false)`, a `::baryon` module registering only `EditorHost`, and a
   replacement `::std::io` module capturing `Arc<Mutex<HostServices>>` (§5). Compile inline with the
   source cap + allocation limiter (startup path).
4. `RuneHost::run_init`: run `pub fn main(editor)` with `(&mut host,)` under the startup budget + host
   quota; project output to `status_message`; bounded diagnostics/`VmError`. **Insert `run_init` between
   session construction and `load_file_into_pane`** (`runtime.rs:2079`) — order: settings → init.rn →
   initial file → loop. Editor always starts. Because registration is absent in this cut, the compiled
   Unit and all returned Rune values are dropped after `main`; successful and failed startup both leave
   `generations` empty and retain no script-owned Rune state.

Follow-on cuts then delivered `SourceStaging` with reject-before-copy caps; registration functions
and `RuneCallbackRef` leases; publication tracking and revision-neutral failed-candidate discard;
payload-routed `RuneHost::invoke`; `:source`/reload/unload with per-source identity and atomic owner
replacement; the bounded off-thread compiler; deferred callback self-modification; and the measured
retained-memory protocol from §8.

## 10. Acceptance tests

**Guarded-borrow tripwires** (run **in child processes** so an abort cannot kill the harness):

- **Stored-callback invocation with a guarded `&mut`.** A `rune::Function` from a minimal unit,
  stored, then called later with a fresh `&mut EditorHost`, mutates the live host and observes its
  writes.
- **Nested-container invalidation.** Returning the editor handle — directly, via `Vec`, object,
  nested tuple — yields a clean `VmError` on reuse, never an abort (spike §2's five paths).

**Resource tripwires — distinct mechanisms, not conflated:**

- `while true {}` → **instruction-budget** cutoff.
- A `println!` spam loop → **host-call-quota** exhaustion (it hits the quota before the instruction
  budget), with the `MessageLog` staying ring-bounded.

**First-cut tests.** `println!("x")` reaches `MessageLog`, not real stdout; compile failure leaves the
editor started with bounded diagnostics and empty `generations`; its startup diagnostic remains
visible after the initial-file load; a source over `MAX_SOURCE_BYTES` is rejected before compile
(read `limit + 1`); invalid-UTF-8 source is rejected; absent `init.rn` is a clean no-op.
**Init-before-file ordering** is asserted via an **internal trace point / test init runner** — not
through Rune buffer inspection, since no editor methods exist yet.

**Registration/source-cut tripwires.** A stale compile result is rejected across
request → unload → same-path re-request (global request IDs prevent ABA); sourcing the configured init
path replaces the `Init` owner rather than creating a second source; different source keys both load;
same-key requests coalesce latest-wins; source-slot, per-key-byte, and aggregate-key-byte limits have
exact-boundary tests; unload cleanup occurs only after `remove_owner` succeeds.

## 11. Non-goals / out of scope

The event bus (item 6); the `baryon::*` buffer/cursor API (item 9); the **plugin-facing** background-
job/subprocess/network API (item 7; the internal source compiler is specified here); a message-log
viewport UI; plugin discovery/manifests (item 10); rune's `emit`/codespan rendering.

## 12. Open decisions for review

1. **Closed by the implementation spike.** Release-mode measurement validated signed retained
   deltas across repeated callback growth (`+1,024`, `+130,048`, `+1,966,080` bytes) and captured
   state release (`-2,097,152` bytes); a retained `Unit` crossed threads and dropped safely. A 4 MiB
   bounded source compiled in approximately 10 ms on the development machine. The implementation
   therefore uses the bounded thread worker with a 512 MiB steady session cap and 256 MiB replacement
   headroom. The permanent accounting tests retain the algebraic multi-source, at-cap replacement,
   pending-reservation, and return-decoding tripwires; the measured constants can be tightened from
   production telemetry without changing the protocol.
2. **Closed in the overlay follow-on:** `OwnerState.published` tracking and checked,
   revision-neutral `discard_unpublished_empty_owner` are implemented and cover every
   post-allocation candidate-failure path.
3. **Closed this pass:** the retained-memory **transaction algebra** (peak/steady admission formulas,
   signed non-transactional callback deltas, pre-compile `R` and pre-evaluation `U` reservations,
   `min(op_cap, steady_free, peak_free)` limiter, return-length preflight before sampling, single
   in-flight pending-Unit rendezvous); and
   the source-identity **ABA fix** — a host-global monotonic `SourceRequestId` (never reset/reused,
   exhaustion-checked) replacing the per-slot counter, plus **init-path normalization** so
   `:source <init path>` maps to `SourceKey::Init`.
4. **Closed earlier:** off-thread read+compile (`Context`/`Unit` are `Send + Sync`, no on-thread
   fallback); per-`SourceKey` latest-wins; fixed option arity; payload routing; incremental staging
   caps; explicit publication state + candidate cleanup; host quotas over init/source/callback; status
   bounds; `run_init` slots between construction and file load without a refactor; init-before-file
   order locked.
5. **Confirmed sound / retained:** Option A (permanent `EditorHost` swap), direct guarded `&mut`, the
   `Function::call` model, the `::std::io` replacement, strict-UTF-8 source handling, bounded
   diagnostics, child-process tripwires, `with_config(false)`, no `emit`.
