# ADR 627 - mid-task worker loss diagnostic

**Status:** PROPOSED. Diagnosis only. Successor to ADR 626, which closed
scoped to its declared campaign.

**This plan authorizes no behaviour change.** It authorizes one
test-only instrument commit and the diagnostic runs below, after the
instrument, the retained ladder, the frozen schedule and the readers'
negative controls have been reviewed. Any bound,
watchdog (`W`) or recovery change needs a separate, reviewed plan built
on this readout.

**A correctness and robustness record. Wi-Fi is a supported target, and
no performance claim is made.**

## Context

**What ADR 626 covered, and what it did not.**
- Every ADR 626 kill happened *between* stages.
  - A producer was killed after its sealed manifest was accepted.
  - A dispatch target was killed before anything was dispatched to it.
- `H` bounds only establishment, and only under the opt-in experiment;
  `W` stayed deferred. So no task was running on the victim when it
  died.
- The deferred dispatch-exclusion shortcut is a separate latency
  question, and is not in scope here.

**What bounds a task that is already running, from the code.** This is a
hypothesis; this plan confirms or replaces it.
1. **The dispatch.** After `dial_worker`, `NetworkTaskDispatcher::dispatch`
   awaits `execute_task_on_worker_with_telemetry_and_batch_sender` with
   no deadline of its own. The only other exit is
   `active_query.stopped()`, when an active query is attached.
2. **The transport.** ADR 577's policy (`toron_client::liveness`) is a
   300 s idle timeout with a 5 s keep-alive. Whether a peer that has
   vanished ends an established stream earlier (for example after an
   ICMP port-unreachable) is **not assumed**. ADR 626 Stage A observed
   dials to a dead worker failing at about 300 s; no established stream
   was observed.
3. **Two coordinator paths behave differently.**
   - **The scheduler path:** the ADR 625/626 probes drive
     `DistributedScheduler::dispatch_loop` directly. No active query and
     no lease renewal are attached.
   - **The production coordinator path:** `ToronCoordinator` attaches an
     `ActiveQueryHandle` and spawns `spawn_query_lease_renewal_task`.
     - Every `DEFAULT_QUERY_LEASE_RENEW_CADENCE` (5 s), it dials each
       remote worker of the query with `REMOTE_CANCEL_CONNECT_TIMEOUT`
       (100 ms), then renews with `REMOTE_CANCEL_ACK_TIMEOUT` (100 ms).
     - The first tick comes one cadence after the task starts, at
       `now + cadence`. `remote_workers` is the growing census of
       workers that remote tasks have been recorded on. Those workers
       are renewed one after another.
     - Each renewal has 100 ms for the connect, then another 100 ms
       for the whole `issue_or_renew_query_lease_on_worker` operation.
       A timeout of that outer operation is not necessarily a
       timeout reading the acknowledgement.
     - A single failure calls `fail_query_lease`: the active query is
       marked cancelled, and the failing worker is retained.
     - An execution already inside the `select!` surfaces as
       `WorkerLost`. A dispatch issued after that can hit the early
       `is_cancelled` check and return `Cancelled` instead. The
       record keeps the error actually surfaced, and does not require
       every dispatcher to report `WorkerLost`.
     - The active query stays stopped, so ordinary retries are not a
       viable recovery on this path.
   - **Hypothesis:** the production path ends a mid-task loss within
     about one cadence, but terminally. That timing depends on the
     cadence and its phase; it is not a bound measured from the kill.
   - Whether the 100 ms deadlines also fail *healthy* queries over
     Wi-Fi is not established: no ADR 625/626 record exercised this
     path across hosts.
4. **A worker that is alive but stalled** (for example `SIGSTOP`) keeps
   its socket, so it sends no ICMP. It is a different failure from a
   killed process. Its transport and lease outcomes are unknown.
5. **An ending is attributed to what was observed:** the typed error or
   event, and any competing stop signal. It is never attributed from an
   elapsed time near 300 s alone.

## Part 1: the instrument (one test-only commit)

`diag: adr 627 mid task loss instrument`. It is laddered on bigfoot (in
a dedicated worktree with the 1.95.0 override), and its executables are
built once, hashed, staged and manifest-verified per host, as in ADR 626
Stage C. Every run uses exactly those executables.

**Where the instrument sits.** Mostly at the transport and stream
boundary, as in ADR 626 Stage A. The lease and worker-side task events
may need `#[cfg(test)]` (or test-feature) hooks inside production
modules. Each hook is passive and named in the instrument commit. No
production timeout, lease, cancellation or recovery behaviour changes.

- **Running-task binding.** The loss probe gains mid-task modes. Before
  the signal, the victim is bound to one exact typed attempt (query,
  stage, task, attempt) on a declared worker and endpoint.
  - The worker must have recorded its own `StartTask` acceptance and
    execution entry for that attempt. A `STREAM_ACCEPTED` alone is not
    enough.
  - No outcome has returned when the signal lands.
  - A dispatch still pending on establishment, lease setup or
    `StartTask` serialization is a different case, reported as such.
  - Ordering is causal, from acknowledgements and single-host clock
    intervals. Receiver timestamps are never subtracted from
    coordinator timestamps.
  - If running-at-signal cannot be established exactly, coverage is
    reported as unavailable.
  - The signal is issued a frozen delay into a task of frozen length.
- **Long tasks to kill into.** Test-only fixture cases that keep a task
  running on the victim:
  - **final-stage:** `slow_scan` (ADR 626's 8 s slow table);
  - **producer:** a join whose producer side scans the slow table, so a
    producer stage task is mid-scan when killed.
  The delay and task length are frozen before use.
- **Failure modes:**
  - `kill`: `SIGKILL` of the victim's process group (ADR 625's
    primitive);
  - `stop`: `SIGSTOP` of the victim's owned group (alive but stalled),
    with its stopped state confirmed for that group. That confirmation
    is separate from `kill`'s confirmed-gone marker. It is held for the
    observation window. The cleanup `SIGKILL` comes after the window,
    and stream endings it causes are recorded as cleanup-induced. A
    lack of response during observation is never reclassified because
    of the cleanup.
- **Coordinator paths:**
  - `scheduler`: the existing `dispatch_loop` probe, with ADR 626's
    experiment on (H 5000 / C 12000) as the current configuration;
  - `coordinator`: the real production entry point. It goes through
    `ToronCoordinator`'s own admission, active-query and lease path,
    never the scheduler probe with a manually attached handle.
    - The exact entry point and configuration are frozen in the
      instrument commit.
    - The record states whether ADR 626's experiment is on that path.
      It is not silently attached in order to compare paths.
    - The two paths may differ in plan and configuration. Every
      difference is recorded.
    - Recorded:
      - the lease task's start and ticks;
      - the worker census and renewal order;
      - each renewal's connect and operation outcomes and durations;
      - initial lease issuance (successful or failed), separately from
        the periodic renewals, where it is exposed;
      - `fail_query_lease`;
      - every surfaced error;
      - the final query result, fully consumed, with any partial
        results.
- **Passive events,** reusing ADR 626 Stage A's instrument at the
  transport and stream boundary (no library change): dispatch start and
  return per attempt; the victim's served streams; every established
  stream's end and its reason; lease renewal attempts on the coordinator
  path. Each run records which future was pending, and how and when each
  wait ended, if it did.
- **Healthy lease control.** The coordinator path runs with no kill,
  across hosts, to count how often a 100 ms renewal fails on healthy
  Wi-Fi. These are counts, not a pass bound.
  - Several ordinary cases can finish before the first tick. A run with
    zero renewals is reported as coverage unavailable for lease
    reliability, never as evidence of healthy renewals.
  - A deliberately long healthy case spans several ticks, with the
    intended remote workers recorded and their renewals observed. Its
    task length and required renewal coverage are frozen before the
    runs.
  - Every early failure, setup and initial-lease failures included, is
    kept and reported separately, never retried away.
- **References.** Each fixture case has a same-path, no-fault reference
  with full result consumption. Healthy references and controls must
  show the same fixture semantics and output as the loss cases they
  serve.

## Part 2: the runs

**Local reproduction** (bigfoot loopback, staged executables): each
combination of failure mode (kill, stop), task role (final-stage,
producer) and coordinator path (scheduler, coordinator) once. Each has a
360 s observation window and a 420 s whole-attempt bound.

**Host runs,** with ADR 625/626's harness discipline: per-attempt gates,
immutable raw evidence, exact reader contracts, verified cleanup and a
STOP rule. The host matrix is deliberately asymmetric (table below).
- **Mid-task kill:**
  - scheduler path: final-stage and producer, each on strix and on
    optiplex;
  - coordinator path: final-stage, on strix and on optiplex.
- **Mid-task stop:** scheduler path, final-stage, on strix and on
  optiplex.
- **Healthy coordinator-path control:** the five ADR 626 cases, three
  each, with no kill; lease renewal outcomes recorded.
- **Healthy long coordinator-path control:** the long case, three
  attempts, with frozen required renewal coverage.
- Link campaigns before and after.

| Fault, role, path | Local | Hosts |
|---|---|---|
| kill, final-stage, scheduler | yes | strix, optiplex |
| kill, producer, scheduler | yes | strix, optiplex |
| kill, final-stage, coordinator | yes | strix, optiplex |
| kill, producer, coordinator | yes | local only |
| stop, final-stage, scheduler | yes | strix, optiplex |
| stop, producer, scheduler | yes | local only |
| stop, final-stage, coordinator | yes | local only |
| stop, producer, coordinator | yes | local only |

No cross-host conclusion is drawn for the local-only combinations.

Each loss attempt has a 360 s observation window and a 420 s bound.
These are diagnostics with no pass bound: the readout states what
happened.

**Readers** reuse ADR 626 Stage C's validators, including:
- identity binding to the attempt;
- run-structure checks;
- exact test proof for local runs;
- only post-signal events as evidence.

New contracts:
- bind the running victim (worker-side execution entry, unresolved at
  the signal) and the signal to it;
- confirm a stop separately from a kill.

**The outcome contract is revised for diagnosis.** These are reportable
outcomes, not reasons to discard an attempt:
- a terminal error;
- a healthy lease failure;
- no terminal return within 360 s;
- a loss without a bound running victim.

These still STOP the campaign:
- structural corruption;
- unverified cleanup;
- an unintended signal or exclusion.

Bookkeeping:
- The observation window's expiry is recorded separately from the 420 s
  lifecycle deadline and from stream endings caused by cleanup.
- Trace files are kept even when there is no final run. Identity
  binding then states what is unavailable.

## Deliverable

A readout, before any behaviour change is proposed:
- for each failure mode, role and path: whether and when the dispatch
  returned, with what outcome, and which wait ended it (stream end,
  idle timeout, lease failure, or nothing within the window);
- the lease-renewal outcome distribution on healthy Wi-Fi;
- whether a stalled worker differs from a killed one;
- what a successor plan should consider: a progress watchdog `W`, the
  lease's 100 ms deadlines, recovery on the coordinator path, and (only
  then) the dispatch-exclusion shortcut.

## Guardrails

- No behaviour change: no bound, timeout, TLS, lease, cancellation or
  recovery change. The instrument is test-only and passive; any hooks in
  production modules are `cfg(test)`-gated and named.
- The dispatch-exclusion shortcut stays outside this record.
- **ADR 577's idle policy and the 100 ms lease deadlines are observed,
  not tuned.**
- **Single-host clocks only.** No cross-host timing claims.
- **Kill and stop act only on the attempt's own recorded process group**,
  with ADR 625's ownership checks. A stopped victim is always
  `SIGKILL`ed and verified gone before cleanup is called clean.
- The schedule, delays and task lengths are frozen before the runs.

## Risks

- **A task may finish before the kill lands.** A binding that does not
  show the task still running at the kill is reported as "no running
  victim", not as a loss result.
- **Healthy lease failures on Wi-Fi** could end coordinator-path
  attempts before the kill. They are counted and reported, not
  retried away.
- **`SIGSTOP`** may leave kernel buffers and the peer in states that
  differ between loopback and hosts. Both are recorded.
