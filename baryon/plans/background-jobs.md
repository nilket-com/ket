# Specification: Background Jobs

Issue-inventory item 7 of `plans/rune-scripting.md` (§3.7): the **plugin-facing** background-job
system — subprocess execution with ownership, cancellation, bounded capture, and completion delivery.
It expands the internal substrate of item 2(b) (per-purpose workers: `PathCompletionController`'s
generation-tagged queue, the serialized `HistoryWriter`, the Rune source compiler) into one governed
job layer, and is deliberately **engine-level and Rune-independent**: delivery rides the existing
lease/gate/dispatcher/drain machinery that commands, subscribers, and autocmds already share.
A thin `plans/rune-jobs.md` binding follows, exactly as `rune-autocmds.md` followed
`event-subscribers.md`.

Inherited and not reopened: the owner model (`RegistryOwnerId`, atomic generation replacement), the
dispatch machinery (`ActionLease` typed payloads, `InvocationGate`, payload-type run-loop routing, the
drain point and its `DrainEvents` self-wake), the bus's one-ingress rule (workers send
`InternalEvent`s; the engine-side handler is the fire point), and §2.9's enforcement split (a host
function cannot be interrupted → per-operation caps are mandatory; job/subprocess timeouts are the
fourth enforcement mechanism, specified here).

## 1. Ground truth this builds on

- **Item 2(b) is per-purpose, not general**: `PathCompletionController` (bounded queue, request
  generation, stale-result rejection, `InternalEvent::PathCompletion`), `HistoryWriter` (serialized
  writer thread), and the source compiler worker (`InternalEvent::RuneCompile`) each own one worker
  and one response variant. None supports plugin ownership, cancellation, subprocess execution, or
  output capture.
- **Delivery infrastructure is complete**: `SubscriberDispatcher`-style dispatch at the drain point,
  `InvocationGate::invoke_guarded`, payload-type routing (thread-local `RuneHost` vs. boxed
  fallback), per-entry VM budgets, `HostEffects` merge, and deferred-lifecycle application all exist
  and are characterized. Jobs add **no new delivery mechanism** — only a new payload class and a new
  completion source.
- **Cross-thread completions enter through the ordered request stream** (`EngineRequest::Internal`),
  and hydration attribution already demonstrates the pattern: worker echoes an id captured at
  admission; the engine-side handler liveness-checks before acting.
- **Sandboxing is an open question** (umbrella §3.11) — this plan must not foreclose it, so
  subprocess policy is explicit and conservative rather than incidental.

## 2. Scope: subprocess jobs only

A **job is a subprocess** in this cut. Not a general async-task system:

- The motivating case (§3.7) is a plugin shelling out to `git`/`ripgrep`.
- Rune code cannot run off the engine thread (the VM is engine-thread-only by the settled host
  design), so "run this script code in the background" is structurally impossible — a job's *work*
  is external; only its *completion handling* is script code, delivered on the engine thread.
- **Network jobs are deferred with sandboxing** (§10). Internal compute jobs (Rust closures) stay
  per-purpose workers as today — they have no plugin-facing surface to govern.

## 3. Identity and ownership

- **`JobId(u64)`** — monotonic, never reused, allocated at spawn admission (the established id
  discipline: `BufferId`/`SubscriptionId`/`SourceId`).
- **Every job has an owner: a `RegistryOwnerId`.** Spawning requires a live owner; the job is part
  of that owner's footprint exactly as its subscriptions are:
  - `remove_owner` / `replace_owner_generation` **cancels all the owner's live jobs** (kill signal +
    delivery suppression). A reloaded plugin never receives its predecessor's completions — the
    same no-cross-generation-delivery rule subscriptions have.
  - Engine shutdown logically cancels every live job, requests immediate physical termination, and
    joins only the supervisors that finish inside the bounded shutdown reap budget (§5c); stragglers
    detach rather than delaying editor exit.
- **A job handle is an id, not a liveness guarantee** (the settled handle discipline): cancel/status
  against a finished or unknown `JobId` is a clean typed error, never a panic.

## 4. Spawn — admission-checked, fully specified up front

```
JobSpec {
  program: PathBuf,               // executed directly — never through a shell
  args: Vec<OsString>,            // bounded count and bytes
  cwd: Option<PathBuf>,           // default: engine cwd at spawn
  stdin: Option<Vec<u8>>,         // write-once, bounded, closed after write
  timeout: Duration,              // clamped to MAX_JOB_RUNTIME
  completion: ActionLease,        // typed payload names the owner-side callback
}
spawn(spec) -> Result<JobId, JobSpawnError>
```

- **No shell interpretation, ever.** `program` + `args` go to `Command` directly; a binding that
  wants `sh -c` must say so explicitly as `program`. This is the sandboxing-compatible default: the
  engine never constructs shell strings.
- **Environment policy is explicit**: the child inherits the editor's environment *minus* a small
  engine-internal denylist (the `BARYON_*` control variables), with no per-job env injection in this
  cut — additive env is easy to add later, subtractive mistakes are not.
- **Admission is preflighted** (nothing partial on failure): live-owner check,
  **`completion` lease owner == spawning owner** (the same nested-owner protection autocmds enforce
  on `RuneAutocmdRef` — a lease naming another owner is rejected at spawn), `MAX_JOBS_PER_OWNER`,
  `MAX_JOBS_TOTAL`, arg/stdin byte caps, the **global buffer budget** (§5a) for the stdin bytes, and
  `JobId` exhaustion. Rejection is a typed `JobSpawnError`; admission never blocks on the child
  (process start happens on a supervisor, and a failed `exec` is reported as a
  completed-with-spawn-error job, not a spawn error — the admission/exec split mirrors the
  resolver's admit-then-complete shape).
- The `completion: ActionLease` is the **whole delivery contract**: jobs do not subscribe to
  anything, and the bus taxonomy is untouched. A job completion is addressed to exactly one owner
  callback — broadcast semantics belong to events; completions are point-to-point.

## 5. Execution — queued/running split, per-job supervision, deadlock-free I/O

- **Jobs are `Queued` until a running slot opens.** Admission (§4) accepts up to `MAX_JOBS_TOTAL`
  jobs session-wide; at most **`MAX_RUNNING_JOBS`** children exist concurrently. Queued jobs wait in
  FIFO order per admission and start as slots free; a queued job holds only its `JobSpec` (its stdin
  bytes already charged, §5a) and no process.
- **One supervisor thread per *running* job** — `MAX_RUNNING_JOBS` bounds supervisor threads, so
  threads track children one-to-one and the earlier pool contradiction (4 waiters ≠ 64 children)
  dissolves. Each supervisor owns its child handle exclusively and multiplexes **stdin writing and
  stdout/stderr draining concurrently** via non-blocking pipes and `poll` (Linux-first): stdin is
  written as the child drains it and closed at EOF; both output pipes are drained continuously, so
  the classic write-stdin-while-child-blocks-on-full-stdout deadlock cannot occur. Supervisors are
  named (`baryon-job-N`). At shutdown they follow §5c's bounded join/detach policy rather than an
  unconditional join.
- **Output capture is bounded per stream**: stdout and stderr each capture up to
  `MAX_JOB_CAPTURE_BYTES` and are additionally bounded by the global budget (§5a); past either
  bound the stream **keeps draining** (never blocking the child on a full pipe) but bytes are
  discarded and the capture is marked `truncated`. Capture is bytes, not text — decoding is the
  consumer's explicit choice (byte-text-semantics discipline).
- **Process-tree termination (Linux-first, explicit):** every child is started in its **own process
  group** (`pre_exec` setpgid); ordinary termination signals the **group** — SIGTERM →
  `JOB_KILL_GRACE` → SIGKILL — so descendants a job spawned die with it and cannot hold the output
  pipes open. **Honestly stated limit:** a process group catches *ordinary* descendants; an
  adversarial child that calls `setsid` escapes the group, and no signal strategy retrieves it —
  the **drain deadline (`JOB_DRAIN_DEADLINE`) is the hang backstop**, not the kill. After the kill
  the supervisor drains to EOF or the deadline, closes its ends, and exits. **On non-Linux
  platforms this cut terminates the direct child only**, stated as an explicit limitation (Baryon
  is Linux-first; the io_uring backend already is).
- **Timeout *enforcement* is supervisor-side; timeout *outcome selection* is not** (§5b): at
  `timeout` the supervisor kills its group and reports; the engine selects. §2.9's rule holds: the
  *engine* never waits on a job; only supervisors block.
- **Resource honesty:** wall timeout and the concurrency cap bound *duration* and *count*, not the
  **child's memory or CPU** — a job can allocate gigabytes in its own process. RLIMIT/cgroup
  enforcement is part of the deferred sandbox/resource-policy layer (§10), recorded so the caps
  table isn't misread as a memory bound on children.

### 5a. Global buffer accounting — session-bounded memory

Per-job caps alone do not bound the session (64 jobs × stdin + two captures is gigabytes). A single
**`MAX_JOB_BUFFER_BYTES`** budget covers every **bulk byte-buffer allocation** owned by a job across
its whole lifetime, and it accounts **allocated capacity, not logical length**. The separately
bounded program, cwd, argument, and fixed record metadata do not consume this bulk-buffer budget:

- **Charges are taken *before* allocating**, sized to the capacity being reserved (a capture that
  grows by doubling charges the doubled capacity, not the bytes written so far). Under-counting by
  length while a `Vec` retains its allocation is exactly the bug this rule forbids.
- **queued stdin** is charged at admission and **held until the stdin buffer is actually dropped**
  (fully written and freed, or released on cancel/suppression) — never released incrementally as
  bytes are written, because writing does not shrink the allocation.
- **live capture** charges capacity ahead of growth; when the global budget cannot cover the next
  growth step, further capture truncates — a job never fails because a *sibling* is memory-hungry,
  it just loses tail output.
- **completed-but-undelivered records** keep their charge until delivery hands the record over or
  suppression drops it.

The budget is a **thread-safe shared RAII reservation** (an `Arc`'d counter whose guards release on
`Drop`), because charges are taken and released on both the engine thread and supervisor threads —
`HostByteReservation`'s discipline, made `Send`.

### 5b. Cancellation and timeout — one authority, engine request order

**The engine-thread job table is the only authority that selects a logical outcome.** Supervisors
are *physical enforcers and reporters*: a supervisor may kill its group (on timeout expiry or on a
control-channel request) and it reports facts — but a report never selects the outcome. Each
supervisor owns a **one-write final report cell** (candidate outcome plus bounded captures), written
after it has reaped the direct child or reached the post-kill drain deadline, plus the ordered
`InternalEvent::JobCompleted` send. A timeout may be physically enforced earlier, but it is only a
candidate outcome in that final report; the engine processes reports **in request order** and
decides. Spawn failure produces the same one final report without a child.

```
Queued  ──cancel (host call)────────▶ Cancelled   (dequeued, no process; terminal)
Queued  ──slot──▶ Running
Running ──cancel (host call)────────▶ OutcomeFixed(Cancelled)  ──reap report──▶ terminal
Running ──timeout report processed──▶ OutcomeFixed(TimedOut)   ──reap report──▶ terminal
Running ──exit report processed─────▶ terminal Completed { status }
```

- **Precedence is engine processing order, stated without overlap:** the *first* outcome-fixing
  transition the engine processes wins; **every subsequent request or report against an
  `OutcomeFixed`/terminal job returns `AlreadyFinished`** (for host calls) or folds into the fixed
  outcome (for supervisor reports). A cancel processed after a timeout report was processed is
  `AlreadyFinished`, full stop — there is no "accepted later but wins if first" ambiguity. The
  physical kill a supervisor performed at timeout expiry does not select `TimedOut`; only the
  engine's processing of the timeout report does. (If a cancel was processed first, that same
  physical kill simply serves the cancel, and the timeout report folds away.)
- `cancel(JobId)` while `Running` fixes `Cancelled` even if the child exits naturally before the
  kill lands — the later exit report folds into the fixed outcome.
- **Handle and signal ownership**: the supervisor thread exclusively owns the `Child` **and is the
  only party that signals its process group** — an engine-side `killpg` against a recorded pgid
  could hit an unrelated process after pid/pgid reuse. Cancel reaches the supervisor through its
  control channel; the supervisor does the killing.
- Exactly one delivery decision ever emerges from the table, whatever the interleaving.

### 5c. Shutdown — a separate, latency-budgeted policy

The graceful TERM → 2 s grace → drain path would regress the instantaneous-`:q` guarantee (the
quit-latency work pinned a ~5 ms engine teardown). Shutdown therefore does **not** reuse the
graceful path:

- **Immediate group SIGKILL** to every live job — no TERM, no grace: the editor is exiting and the
  children's cleanup is not its concern.
- **A small bounded reap budget** (`JOB_SHUTDOWN_REAP_BUDGET`, total across all jobs): supervisors
  that finish reaping within it are joined; the rest are **detached** — threads left to finish
  draining/reaping on their own as the process exits (their shared budget guards just drop).
  Pathological stragglers (a `setsid` escapee holding a pipe) cannot stall quit.
- **Logical vs. physical, distinguished:** *logical cancellation is infallible* — the table marks
  every job terminal and suppresses every delivery, unconditionally. *OS termination is not* — a
  kill can fail (ESRCH races) or a reap can stall; those affect only the detached cleanup, never
  the table's state or the quit path.
- **Quit-latency regression test**: a live child that ignores SIGTERM (and a grandchild holding
  stdout) must not push the typed-`:q` median past the existing quit-latency bound.

## 6. Completion delivery — the existing drain path, a new payload class

```
JobOutcome = Exited { status: i32 } | Signaled | TimedOut | Cancelled
           | SpawnFailed { error: BoundedJobError }
JobCompletion {
  job: JobId, owner: RegistryOwnerId, outcome: JobOutcome,
  stdout: JobCapture, stderr: JobCapture,   // bytes + truncated flag
  runtime: Duration,
}
```

`BoundedJobError` is a typed error category plus a UTF-8-safe message capped at
`MAX_JOB_ERROR_BYTES`; an OS error, contextual program path, or dispatcher diagnostic can therefore
never turn one completion record into an unbounded allocation.

1. The supervisor sends **`InternalEvent::JobCompleted(JobId)`** through the ordered request
   stream — one ingress authority, payload minimal; the full `JobCompletion` record lives in the
   engine-side job table (no large payloads through the channel). The send uses the **source-worker
   discipline**: on `Full`, bounded retry with backoff and a **shutdown escape** (a terminating
   engine must never leave a supervisor spinning on a dead channel). **"Arrival order" therefore
   means successful ordered-stream enqueue order**, not subprocess exit-clock order — the only
   order the engine can actually observe.
2. The engine-side handler resolves the id against the job table, **suppresses if cancelled or the
   owner died in flight** (the hydration-attribution pattern), and marks the completion deliverable.
3. **The drain coordinator gains an explicit, persistent completion phase.** Subscriber passes can
   suspend on their step budget, so "dispatch after the events" cannot mean "append to whatever
   pass is current" (that lands completions mid-pass) and cannot mean "wait for full quiescence"
   (unbounded latency). The coordinator's persistent state machine becomes:

   ```
   event pass (to completion, resumable) → captured completion batch → next event pass → …
   ```

   When an event pass fully completes, the coordinator captures the **batch of completions
   deliverable at that boundary** and dispatches them — under the **same shared step budget**, with
   the same suspend/resume and `DrainEvents` self-wake — before capturing the next event pass.
   Completions arriving mid-phase wait for the next boundary. This preserves both properties: a
   completion is never observed between two events of one pass, and latency is bounded by the
   drain's own progress guarantee (budget + self-wake), the same bound events already have.
4. Dispatch goes through a **new language-neutral seam** — `SubscriberDispatcher` cannot accept a
   job completion, and overloading it would tangle the contracts:

   ```
   JobDispatcher<C> {
     fn owner_is_live(&self, owner: RegistryOwnerId) -> bool;
     fn complete(&mut self, context: &mut C, invocation: JobInvocation,
                 completion: &JobCompletion) -> SubscriberOutcome;
   }
   JobInvocation { job: JobId, lease: ActionLease }   // owned, like SubscriberInvocation
   ```

   sharing `InvocationGate::invoke_guarded`, the outcome shape (`SubscriberOutcome` — bounded
   status, `needs_render`, `Delivered | Failed`), and the run-loop router pattern: a payload
   downcasting to the Rune job-callback type routes to the thread-local host (a full VM entry with
   budgets/effects/deferred-lifecycle); anything else goes to a boxed fallback `JobDispatcher`
   (Rune payloads fail closed until `rune-jobs.md`, mirroring the autocmd rollout). Within a
   batch, delivery is enqueue order.
   - **An empty event pass is still a boundary**: when the coordinator finds no events pending, the
     completion phase runs anyway — completions must deliver on an otherwise idle editor, not wait
     for the next unrelated event.
5. **Delivery is exactly-once, then the job record is dropped** (releasing its buffer charge, §5a).
   `Cancelled` jobs deliver nothing (cancellation is the caller's own act), and dead-owner
   completions deliver nothing. Everything else — including `SpawnFailed` and `TimedOut` —
   delivers, because the owning plugin must learn the outcome.
- **Callback-raised events and jobs**: a completion callback may push bus events (next-pass, as
  everywhere) and may spawn further jobs. Chains are bounded per drain by the shared step budget
  plus `MAX_JOBS_PER_OWNER`; the terminal backstop remains the binding's budget, as with autocmd
  wake chains.
- **Owner lifecycle is one coordinated seam.** `RuntimeRegistries::remove_owner`/
  `replace_owner_generation` know nothing about jobs, and must not grow job knowledge (the registry
  stays capability-generic). Instead the run-loop level owner-lifecycle coordinator — the same
  layer that already sequences registry retirement against the Rune host's generation teardown —
  orders: **successful registry retirement first** (making delivery impossible: suppression checks
  owner liveness), **then infallible logical job cancellation** (table transition and delivery
  suppression need no rollback), followed by best-effort supervisor termination signals. Physical
  process termination can fail or outlive the shutdown budget as §5c records; it never revives a
  delivery. A registry failure therefore never leaves jobs logically half-cancelled.
- **`JobRuntime` lives on `EditorSession`**, beside `events` and `subscribers`: Rune host functions
  reach it through `EditorHost`'s guarded session, the owner-lifecycle coordinator and completion
  handler reach it through the session the run loop already holds, and shutdown reaps through the
  same field. One table, four reachers, no second authority.

## 7. Ordering guarantees (the §3.7 question, answered)

- **Job completions never interleave into the bus's event ordering** — they are a separate delivery
  phase at the same drain point, dispatched only at event-pass boundaries (§6.3). A completion
  cannot be observed "between" two events of one committed pass, even when that pass suspended on
  its step budget.
- **Completion → callback latency is bounded by the drain's progress guarantee** (shared step
  budget + `DrainEvents` self-wake), not by polling and not by full quiescence: the `InternalEvent`
  wakes the loop, and every boundary the coordinator reaches drains the pending batch. No timer, no
  busy-wait, no unbounded deferral.
- **No ordering between two jobs' completions beyond enqueue order** — "arrival" is successful
  ordered-stream enqueue, the only order the engine can observe; subprocess exit-clock order is not
  a meaningful concept across racing children. A plugin needing sequencing chains spawns from
  completions.
- Stale-result rejection (2(b)'s generation pattern) is subsumed by ownership: a reload that
  invalidates old work *is* `replace_owner_generation`, which cancels the old generation's jobs.

## 8. Testing (engine-level, a test dispatcher and a helper binary — no Rune)

- Admission: per-owner/session caps and the global buffer budget reject before any process exists;
  dead-owner spawn rejects; a completion lease naming a **different owner** rejects (nested-owner
  protection); arg/stdin caps reject; `JobId`s are monotonic and never reused across owner reloads.
- Queue/slots: `MAX_RUNNING_JOBS + k` admitted jobs run exactly `MAX_RUNNING_JOBS` children with `k`
  queued, starting in FIFO order as slots free; a queued job cancels without ever having a process.
- Execution and I/O: a helper binary echoes stdin→stdout, exits with a requested status; capture
  round-trips bytes; **a child that writes megabytes before reading stdin completes without
  deadlock** (concurrent stdin/output multiplexing); over-cap output truncates with the flag set
  and the child never blocks on a full pipe; timeout terminates within the grace window and reports
  `TimedOut`.
- Spawn failure: its typed category and UTF-8-safe message round-trip, and contextual errors longer
  than `MAX_JOB_ERROR_BYTES` are truncated before entering the completion table.
- Buffer accounting: charges reflect **allocated capacity** (a capture whose `Vec` doubled charges
  the doubled capacity — asserted against the counter); stdin charge persists until its buffer
  drops, not as bytes are written; concurrent captures cannot exceed `MAX_JOB_BUFFER_BYTES` (global
  truncation observed); undelivered-record charges release after delivery; guards released from
  supervisor threads settle correctly (the RAII budget is `Send`); the counter round-trips to zero
  when the table empties.
- Cancellation/timeout authority: cancel while `Queued` → `Cancelled` with no process; cancel
  processed during `Running` fixes `Cancelled` **even when the child exits naturally before the
  kill lands**; a timeout report processed first → `TimedOut`, with a later cancel returning
  `AlreadyFinished`; a cancel processed first → `Cancelled`, with the supervisor's timeout kill
  serving it and the report folding away; the report cell is one-write; exactly one recorded
  outcome and at most one delivery in every interleaving.
- Process tree: a helper that spawns a grandchild holding stdout open — group termination kills
  both and the supervisor's drain deadline bounds the join (assert via pidfile/pgid probe); only
  the supervisor ever signals the group.
- Shutdown: a live child that **ignores SIGTERM** plus a pipe-holding grandchild do not push the
  typed-`:q` median past the existing quit-latency bound; the table is logically terminal for every
  job at exit regardless of OS termination outcomes.
- Ownership: the owner-lifecycle coordinator retires the registry **then** sweeps jobs; a registry
  retirement failure leaves jobs untouched; `replace_owner_generation` kills and suppresses the old
  generation's jobs while the new generation spawns fresh.
- Delivery phase: completions dispatch through `JobDispatcher` + the gate exactly once, only at
  event-pass boundaries — a completion arriving while an event pass is suspended (step budget)
  delivers after that pass **fully** completes, never mid-pass; **an idle editor with no events
  still delivers** (empty pass is a boundary); batch order is enqueue order; a `Full` request
  channel is retried with the shutdown escape and completions still deliver; a `Failed` callback
  outcome doesn't affect other jobs; a non-Rune payload reaches the boxed fallback while Rune
  payloads fail closed.
- Both resolver backends (the suites' established matrix), since delivery shares the run loop.

## 9. Constants (initial; pinned at the integration gate)

| Limit | Value | Basis |
|---|---|---|
| `MAX_JOBS_PER_OWNER` | 16 | a plugin running more concurrent children is a bug |
| `MAX_JOBS_TOTAL` (queued + running + undelivered) | 64 | bounds the whole table |
| `MAX_RUNNING_JOBS` | 16 | concurrent children == supervisor threads, one-to-one |
| `MAX_JOB_CAPTURE_BYTES` (per stream) | 8 MiB | `git diff`-sized output fits; a runaway stream truncates |
| `MAX_JOB_BUFFER_BYTES` (global, §5a) | 64 MiB | session-bounds stdin + captures + undelivered records |
| `MAX_JOB_RUNTIME` (default and ceiling) | 120 s | callers may request less, never more |
| `JOB_KILL_GRACE` | 2 s | SIGTERM→SIGKILL window (graceful path only, never shutdown) |
| `JOB_DRAIN_DEADLINE` | 2 s | post-kill pipe drain bound; the hang backstop for group escapees |
| `JOB_SHUTDOWN_REAP_BUDGET` | 50 ms total | bounded reap at quit; stragglers detach (§5c) |
| `MAX_JOB_ARGS` / `MAX_JOB_ARG_BYTES` | 256 args / 128 KiB aggregate | admission-checked |
| `MAX_JOB_PROGRAM_BYTES` / `MAX_JOB_CWD_BYTES` | 4 KiB each | admission-checked path bounds |
| `MAX_JOB_STDIN_BYTES` | 8 MiB | admission-checked, charged against §5a |
| `MAX_JOB_ERROR_BYTES` | 4 KiB | bounds spawn/OS diagnostics retained in a completion record |

## 10. Non-goals / deferred

- **The Rune binding** (`baryon::jobs::spawn(...)`, callback registration shape, per-spawn callback
  retention and its accounting) → `plans/rune-jobs.md`. The binding must solve one problem this
  plan deliberately leaves to it: completion callbacks referenced at *spawn time* (callback-time)
  rather than evaluation time, which the generation callback table does not currently permit.
- **Network jobs, per-job env injection, ptys/interactive stdin, streaming (incremental) output
  delivery** — streaming needs per-chunk delivery policy; this cut is completion-only.
- **Sandboxing** (§3.11) — the no-shell, explicit-env, admission-gated design is chosen to compose
  with a future manifest-based policy, not to implement one.
- **Migrating 2(b)'s internal workers onto `JobRuntime`** — possible later hygiene, not load-bearing.

## 11. Implementation order

1. **Job table + ids + admission + buffer accounting** — `JobId`, `JobSpec` validation, all caps,
   owner/lease validation, the §5a capacity-charging `Send` RAII budget; table-level tests.
2. **Supervision** — queued/running slots, per-job supervisor with poll-multiplexed stdin/output,
   process groups, TERM→grace→KILL, drain deadline, the one-write report cell; helper-binary tests
   without delivery.
3. **Outcome authority** — the §5b engine-order state machine over reports and host calls,
   `AlreadyFinished`; the linearization tests.
4. **Shutdown policy** — immediate group SIGKILL, bounded reap budget, detached stragglers,
   logical-terminal table; the quit-latency regression with a TERM-ignoring child.
5. **Ownership sweep** — the run-loop owner-lifecycle coordinator (registry retirement → infallible
   logical cancellation → best-effort physical signal), `replace_owner_generation` integration.
6. **Completion path** — `InternalEvent::JobCompleted` with retry/shutdown-escape, engine-side
   suppression, `JobDispatcher<C>`/`JobInvocation`, the drain coordinator's **completion phase**
   (shared budget, suspend/resume, self-wake, empty-pass boundary) with a recording test
   dispatcher; boundary/ordering tests.
7. **Run-loop routing** — payload-type arm beside subscribers (Rune payloads fail closed until
   `rune-jobs.md`, mirroring the autocmd rollout).
8. **Integration gate** — full matrix through the real engine loop; calibrate §9.
