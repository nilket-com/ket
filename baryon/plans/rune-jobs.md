# Specification: Rune Jobs (`baryon::*`)

The Rune binding over the sealed engine job substrate (`plans/background-jobs.md`, implementation
`3456add`). It closes the last deliberately deferred problem in that plan: a completion callback is
created **during** a Rune callback, while the generation callback table and retained-memory accounting
are owned by `RuneHost`.

This is a thin binding. It inherits and does not reopen subprocess execution, direct `program + args`
execution (no implicit shell), owner-scoped cancellation, queue/running limits, timeout/cancellation
linearization, process-group shutdown, output capture, global job-buffer accounting, or drain ordering.
It defines only Rune values, callback retention, two-phase admission, completion delivery, and the
small handle surface.

## 1. API: a specified job plus an explicit completion callback

```rune
let spec = baryon::JobSpec::new("git", ["status", "--short"])?;
let spec = spec.with_cwd("/work/tree")?;
let spec = spec.with_stdin(input_bytes)?;
let spec = spec.with_timeout_ms(5_000)?;

let job = baryon::spawn(editor, spec, |editor, job, result| {
    if result.outcome() == JobOutcome::Exited(0) {
        let stdout = result.stdout(editor)?;
        // consume explicit bytes
    }
});
```

- `JobSpec` is a Rune-owned, opaque value: UTF-8 `program`, a Rune vector of UTF-8 arguments, optional
  UTF-8 working directory, optional `Bytes` stdin, and a millisecond timeout. Its constructors/builders
  are pure value operations; `spawn` is the sole stateful call. The binding deliberately exposes UTF-8
  paths/arguments only. The engine's `OsString` capability remains internal rather than inventing an
  ambiguous byte-to-platform-string conversion.
- `baryon::spawn(editor, spec, completion) -> Result<Job, JobError>` accepts exactly one `Function`.
  Its callback ABI is `fn(editor, job, result) -> ()`. `Job` is an opaque `{ owner: RegistryOwnerId,
  id: JobId }` equality/debug/clone token, never a child handle. The callback receives the same token
  the spawning call returned.
- `Job::cancel(editor) -> Result<(), JobError>` and `Job::status(editor) -> Result<JobStatus,
  JobError>` are the only handle operations in this cut. `JobStatus` is `Queued`, `Running`, or
  `CancellationPending`; a completed/delivered/suppressed record is no longer queryable. A handle is
  valid only while its owner is the active Rune generation; cross-owner, stale, and finished cases are
  typed errors. There is no `await`,
  polling loop, output streaming, stdin-after-spawn, shell helper, environment injection, or process
  signal API.
- A job completion is an invocation-scoped `JobResult`, passed by guarded reference. It exposes:
  `outcome() -> JobOutcome`; `runtime_ms() -> u64`; `stdout(editor) -> Result<Bytes, JobError>` and
  `stderr(editor) -> Result<Bytes, JobError>`; and `stdout_truncated()/stderr_truncated() -> bool`.
  `JobOutcome` is typed: `Exited(status)`, `Signaled`, `TimedOut`, or `SpawnFailed`. Cancellation is
  deliberately not delivered by the engine, so it is not a callback outcome. `SpawnFailed` exposes a
  bounded `JobErrorKind` and bounded message accessor. `JobResult` has no `Clone` protocol; the guard
  invalidates a retained/direct/nested escaped result after callback return, as for `EditorHost` and
  autocmd snapshots.
- `JobError` is the one expected-error type. Its equality-free `kind()` covers malformed spec,
  admission (`TooMany…`, `BufferBudgetExhausted`, `RuntimeUnavailable`), stale/cross-owner handle,
  `AlreadyFinished`, and completion-output quota failure. Small numeric details only are inspectable.
  A host quota exhaustion is a VM panic, consistent with the existing `baryon::*` contract; no
  user-controlled error uses Rust panic.

## 2. Where jobs may be created

`spawn`, `cancel`, and `status` require an active callback owner recorded on `EditorHost`; source
evaluation and startup `main(editor)` have no published live owner and reject them. This prevents a job
from being attributed to an unpublished generation. The active owner is established by every command,
mapping, option, autocmd, and job-completion invocation, and cleared on all exits.

Every stateful function charges the existing HostBudget call counter before native work. `spawn` also
preflights the Rune-side structural inputs before cloning/converting: argument count and total bytes,
program/cwd byte lengths, stdin byte cap and timeout conversion. `JobSpec` builders do not traverse or
copy unbounded host data; Rune instruction/allocation limits bound their pure construction.

Completion `stdout`/`stderr` copy only the already bounded engine capture into Rune `Bytes`. They charge
the host read-byte quota before allocating/copying, settle to actual copied bytes, and retain the engine
capture's byte-exact discipline. A callback that does not request output pays no read-byte charge and
does not copy it into Rune memory.

## 3. The load-bearing protocol: reserve now, retain and commit after return

The callback passed to `spawn` cannot be appended to `Generation.callbacks` at the native call site:
the currently executing `Function` is borrowed from that same generation, and a captured `Function`
must be accounted under Rune's allocation limiter. Nor can `spawn` return only a future admission
result: the engine contract allocates monotonic `JobId`s at admission and the Rune API returns a real
handle.

Therefore the binding adds a narrow two-phase engine seam, used only by the host:

```
reserve_job(owner, prepared_spec) -> JobReservation { id, … }
commit_reserved_job(reservation, ActionLease) -> JobId
discard_reservation(reservation)
```

`reserve_job` is the linearized admission point. It performs all current `JobRuntime::spawn` preflight,
allocates the never-reused `JobId`, reserves the owner/session slot and stdin buffer charge, and holds a
non-running `Reserved` record. No process exists and no completion can deliver before commit.
`commit_reserved_job` validates the owner-matched lease and transitions exactly that record to the
existing queued state; it is infallible after its preflight. Dropping/discarding a reservation removes
the record and releases every held engine buffer charge. Reservations count toward the same owner and
session job caps, so a callback cannot evade admission by staging many jobs.

At `baryon::spawn`:

1. The host verifies active owner and preflights/converts `JobSpec` without launching a process.
2. It calls `reserve_job`, obtains the durable `JobId`, and pushes
   `DeferredRuneJob { reservation, completion: Function }` into an `EditorHost` callback-local vector.
   The vector has explicit count and metadata/input-byte caps; any push failure discards the reservation
   before returning its typed error. `Job { owner, id }` returns immediately.
3. `Job::cancel` against a callback-local reservation discards it and makes later status/cancel a typed
   terminal/unknown-handle error; against an already committed id it delegates to `JobRuntime`. A
   reserved job's visible status is `Queued`, accurately meaning admitted but not running.

After `Function::call` returns, `RuneHost` temporarily owns/removes the invoking `Generation` from its
map, ending the borrow of its current callback. **Still inside the same `limit::with` allocation scope,
before sampling `remaining_after`,** it drains `DeferredRuneJob`s in FIFO order:

- if the callback's runtime-registry owner is still live and the reservation is active, move
  `completion` into that temporarily owned generation's callback vector, construct
  `RuneCallbackRef { owner, callback_id }`, and commit the reservation with that lease;
- otherwise discard the reservation and drop the function.

The generation is always restored to the map before deferred source lifecycle actions run. This makes
the closure capture's allocation/deallocation part of the **same signed delta** already applied to
`Generation.retained_bytes` and `RetainedRuneBudget.charged`: accepted callback functions remain
retained; failed/cancelled deferred functions are dropped before measurement and do not leave phantom
charge. There is no second callback-memory budget, no unmeasured post-call append, and no `unsafe`.

Deferred lifecycle order is intentional: finalize job reservations first, settle the signed allocation
delta, then apply `source`/`unload` actions. A self-unload subsequently uses the existing
owner-removal path, which cancels/suppresses any job just committed for that owner and drops the whole
generation (including its new completion function). This is non-transactional but coherent: an accepted
spawn can be immediately cancelled by a later accepted unload in the same callback; no callback can
outlive its generation. Errors from finalization/accounting/lifecycle use the existing callback outcome
precedence and cannot leave a reservation or `Function` stranded.

## 4. Completion routing and VM-entry semantics

`RunLoopJobDispatcher` replaces its current `RuneCallbackRef` fail-closed arm with
`RuneHost: JobDispatcher<EditorHost>`. It validates that the lease payload is a `RuneCallbackRef` whose
owner equals `JobInvocation.owner`, resolves the retained function in that live generation, and never
falls through to the boxed `JobDispatcher<EditorSession>`. Every non-Rune payload still takes the boxed
fallback path unchanged.

The call is a full VM entry, parallel to dynamic commands and autocmd event callbacks:

- reset all HostBudget counters and `HostEffects`;
- use the generation's `callback_allowance` and Rune instruction budget;
- pass `(&mut EditorHost, Job, &JobResult)` through `GuardedArgs`;
- apply signed retained-memory delta even if Rune returns `VmError`;
- finalize any callback-local job reservations before measuring that delta;
- apply deferred source lifecycle after the generation has been restored; and
- merge `HostEffects.needs_render` and one bounded foreground status through the existing
  `SubscriberOutcome`/job-drain path.

The completion record is removed from `JobRuntime` exactly once before callback invocation, as already
specified by the engine plan. Callback failure does not retry, respawn, or resurrect it; the bounded
failure is reported through the job dispatcher and the drain continues. A completion callback may spawn
new jobs: they are reserved/finalized by the protocol above and their later completions enter the
existing next drain boundary, never re-enter the active delivery.

## 5. Ownership, reload, and cancellation

The owner is the source generation, not the source path and not a `Job` token. Consequently:

- source reload replaces the registry owner and invokes the existing `cancel_owner` path; old jobs are
  logically cancelled and their completed records suppressed before new-generation callbacks can run;
- explicit unload has the same effect; a late supervisor report is folded by the engine table;
- a `Job` captured/retained by another Rune closure cannot cross this boundary: every operation compares
  `job.owner` with `EditorHost.active_callback_owner`, then the job table verifies ownership again;
- job completion callbacks are dynamically appended only to their owning generation. Owner teardown
  drops them, releases their accounted retained memory, and leaves no separate liveness map.

This is deliberately stricter than path-based lookup. Close/reopen/reload never lets an old handle
target a new generation or child process.

## 6. Bounds and effects

All subprocess/resource constants remain inherited from `jobs.rs`: 16 jobs per owner, 64 session jobs,
16 running children, bounded args/stdin/captures/runtime, and the 64 MiB shared engine job-buffer budget.
The binding adds only bounded callback-local staging:

| Limit | Initial value | Purpose |
|---|---:|---|
| `MAX_DEFERRED_RUNE_JOBS` | 16 | No more pending Rune jobs in one callback than the per-owner live-job cap. |
| `MAX_DEFERRED_RUNE_JOB_METADATA_BYTES` | 128 KiB | Bounds host-side converted spec metadata while callbacks unwind. |
| completion output copying | existing Host read-byte quota | Bounds Rune-side copies of stdout/stderr per delivery. |

`MAX_DEFERRED_RUNE_JOBS` is checked before moving a callback `Function`; metadata is checked before
Rust conversion/allocation; engine reservation is checked before a `Job` is returned. The callback's
captured state remains subject to the generation/session retained-memory algebra, not this metadata cap.

`spawn` itself does not render. A completion callback that edits through the existing buffer API sets
`HostEffects.needs_render`; job drain merges it once, preserving the input contract's render batching.

## 7. Acceptance and regression tests

1. **Real VM ABI:** a script spawns a direct executable with UTF-8 args and receives `Job`, guarded
   `JobResult`, typed exit status, byte-exact stdout/stderr, truncation flags, and runtime.
2. **Reservation correctness:** `spawn` returns a monotonic real id before callback return; no child
   starts before finalization; a host-side staging rejection, VM error, and self-unload each release the
   reservation and leave engine job-buffer charge and Rune retained charge at baseline.
3. **Retained capture accounting:** a completion closure captures allocation; accepted spawn raises its
   generation charge, unload/reload returns it to baseline, and a failed/cancelled reservation does not
   leave positive phantom charge. Run the escape/retention case in a child process under `panic=abort`.
4. **Owner isolation:** source reload/unload cancels an old job and suppresses its callback; an old
   `Job` used by another generation is a typed cross-owner/stale error; a malformed lease with a nested
   foreign `RuneCallbackRef` fails closed and never reaches the fallback dispatcher.
5. **Lifecycle ordering:** `spawn; unload!` in one callback produces no later callback; a completion
   callback that spawns another job delivers the second only at a later drain boundary.
6. **Resource bounds:** exact boundaries for deferred-job count/metadata, args/stdin/timeout inherited
   errors, and stdout/stderr copy exhaustion without an unbounded allocation. A callback that requests
   no output makes no Rune-side output copy.
7. **Cancellation/status:** cancel before finalization, queued/running cancel after finalization, and
   terminal/cross-owner status errors are typed and cannot affect another owner.
8. **Fallback compatibility:** a non-Rune job lease continues through the boxed dispatcher unchanged;
   a Rune lease never falls through.
9. Full lib suite, default and `BARYON_SVP_BACKEND=blocking` serialized integration gates, clippy
   `--all-targets`, formatting, and the existing quit-latency regression with live jobs.

## 8. Non-goals and follow-ons

- Streaming output, job input after spawn, process inspection/signalling, environments, shell parsing,
  network jobs, cgroups/RLIMIT/sandbox policy, and script background execution remain out of scope.
- The engine's completion order/cancellation/timeout/process-tree semantics stay exactly those of
  `background-jobs.md`; this plan has no event-bus subscription or autocmd registration component.
- A future richer job API may add a bounded status/result history. It must preserve owner scoping and
  cannot retain completed output beyond the engine's current delivery lifetime without a new accounted
  storage contract.
