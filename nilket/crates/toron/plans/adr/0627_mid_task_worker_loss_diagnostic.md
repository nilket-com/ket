# ADR 627 - mid-task worker loss diagnostic

**Status:** CLOSED. Diagnosis delivered, WITH LIMITED lease coverage
(see [Results](#results-reviewed)). The frozen long healthy coverage
was **not met** in any long attempt. That is a reported diagnostic
result, not a robustness acceptance. Successor to ADR 626, which closed
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

### Instrument amendment (reviewed)

Recorded before any frozen evidence.

**Lease observation is indirect, by design.** There is no production
hook and no cargo feature. The probe's `ObservedTransport` records every
lease operation as dial events, frame events (`QueryLease`,
`QueryLeaseAccepted`) and stream ends.
- **Observed:**
  - the order of renewal dials, per worker;
  - each dial's outcome and elapsed time;
  - whether an ACK was read.
- **Not observable, and never reconstructed from the dial sequence:**
  - the renewal timer's actual ticks;
  - complete census snapshots;
  - the moment `fail_query_lease` fires.
- **Classification uses the dispatch tag and the complete stream
  sequence.**
  - **Issuance:** `QueryLease` then `StartTask` on the same stream.
  - **Incomplete issuance:** a dispatch-tagged stream with `QueryLease`
    but no `StartTask`.
  - **ACKed lease-only stream (renewal candidate):** an untagged stream
    that wrote only `QueryLease`, read its ACK, and ended without
    `StartTask`.
    - This is strong observational evidence of a periodic renewal.
    - But a dispatch cancelled after its lease ACK and before
      `StartTask` produces the same sequence on the coordinator path, and
      the wrapper cannot prove which caller owned the stream. So it is
      never called a positively identified renewal.
  - **Ambiguous lease-only:** no ACK.
  - **Unresolved:** a lease-only stream that has not ended.
  - **Unclassified dial:** no frame written.
  - **The frozen operational coverage metric** for the long healthy
    case is the per-worker count of ACKed lease-only streams in clean,
    completed runs. It is not direct proof of the timer or the caller.
- **An ACK observed is not whole-operation success.**
  `issue_or_renew_query_lease_on_worker` calls
  `finish_terminal_exchange` after reading it, within the same 100 ms
  bound, so operation completion stays unavailable. A dropped stream is
  evidence of cancellation, not of which timeout fired.
- **The lease-failure inference has two levels,** ordered on the probe
  clock.
  - **Consistent with lease failure:** a structurally recorded
    `WorkerLost` naming the target (from `TaskFailureKind`, never error
    text), and a failed post-signal lease operation to the target before
    the victim stream ended and before the terminal stamp. The
    operation's caller (renewal or issuance) is not proven, so nothing
    stronger is claimed.
  - **Unresolved:** anything else, including `Cancelled` alone, another
    worker, the wrong order, or no terminal return.
  - `fail_query_lease`'s own timing is always unavailable.
- **Dials abandoned before resolving** (an outer timeout) are recorded as
  `dial_abandoned` with their elapsed time, by a drop guard in the
  observer.
- **WARN-and-above tracing events** (for example "query lease renewal
  connect failed") are copied into the probe log by a test subscriber.
  The renewal loop's timeouts emit no warning.

**The mid-scan binding.** It is settled from the victim's own log, read
after cleanup.
- **Test-only events:** the fixture worker's slow and long scans emit
  `scan_entered` and `scan_returned`, tagged with the served stream
  through the `SERVED_STREAM` task-local.
- **"mid_scan" requires all of:**
  - exactly one served `StartTask` for the victim attempt;
  - that stream's `scan_entered`;
  - no `scan_returned` and no task output frame (`TaskBatch`,
    `TaskComplete`, `TaskFailed`, `TaskCancelled`) written on it;
  - on the probe: no task output read on the victim stream before the
    signal;
  - a confirmed signal: gone for a kill; stopped, every owned member in
    state T, for a stop.
- `TaskStarted` is an acknowledgement, recorded but not disqualifying.
- Anything else is a named coverage-unavailable outcome:
  - an unbound entry (task-local lost);
  - a scan that returned (a different work boundary);
  - a victim that landed elsewhere (never retargeted).
- **The trigger is fixed:** 3 s after the victim's `StartTask` payload
  is written, on the probe's clock. It is not "3 s into the scan".
- **The victim stage is declared, then verified:** stage 0 for
  `slow_scan` and `slow_join` on both paths. That stage was observed in
  the same-path loopback references, and is checked after each run
  against the victim's own `scan_entered`. The subscription is installed
  before dispatch and is unbounded (no lag); a closed log ends the watch
  without a victim.

**Every conclusion is recomputed from raw events.**
- Each event's fields are validated by name, and an unknown event
  refuses.
- On the probe:
  - the victim stream's dial (target and endpoint);
  - exactly one `StartTask` write for the victim attempt (timed exactly
    as saved, before the signal, one query);
  - output before the signal, recomputed from raw frames;
  - exactly one `signal_sent` and one `signal_returned` for the target
    and fault;
  - the confirmation, recomputed from the primitive's raw result: the
    declared command, exit 0 and the JSON marker on hosts; the `/proc`
    state locally.
- On the worker:
  - stream acceptance, then exactly one `StartTask` read, then exactly
    one slow-table `scan_entered`;
  - no return and no output.
- A saved summary that disagrees with the raw events refuses.
- Every instant is captured once and both saved and emitted with that
  value.
- The raw `signal_returned` result must equal the saved signal result,
  and both signal times must equal their raw stamps. Exit statuses are
  exact integers (`False` is not exit 0).
- Terminal timing (`query_returned` or `observation_expired`) is stamped
  inside the query future, never after the watcher completes. Its `ok`
  must match the saved outcome: true for completed, false for an error;
  `observation_expired` for no terminal return.
- References receive the same raw-terminal validation before they
  become parity anchors. A missing or contradictory terminal refuses.
- The StartTask-to-signal interval is reported against the frozen 3 s;
  any excess is reported, not hidden.
- Errors carry a typed `error_task_failure` (taxonomy label, plus the
  lost worker's id for `WorkerLost`).
- The scheduler's completed runs pass ADR 626 Stage C's full
  run-structure and recovery-identity checks.
- A reference that does not complete cleanly is a named incomplete
  reference, and no parity is drawn from it.

**Logs.**
- Each event is one complete line written with `write_all`; a failed
  write prints `wait event log write failed`, which the readers treat as
  incomplete instrumentation.
- An event that was not persisted is never delivered to an observer;
  the probe does not signal after an instrument failure.
- Only a final, incomplete line (a process killed mid-write) is dropped
  and flagged as a truncated tail. Any other malformed line refuses.
- A truncated tail withholds every absence-based conclusion that depends
  on that log, such as "mid_scan" (from either the victim's log or the
  probe's) and the ending and lease conclusions (from the probe's). That
  is reported as coverage unavailable, naming which log prevented it.

**The long healthy case is `long_join_last`** (`customers JOIN long`,
with the `long` scan frozen at 30 s).
- **Observed on loopback:** both producers are dispatched together, so
  two workers are in the census, and renewed, through the long scan.
  The third joins with the consumers afterwards.
- `long` first (`long_join`) dispatched serially and kept one worker in
  the census; a three-way join gave no more.
- **Frozen required coverage:** at least 5 renewals observed to each of
  at least 2 workers. Shorter healthy cases with no renewal are reported
  as "lease coverage unavailable".

**Paths.**
- The coordinator path is `DistributedQueryCoordinator::execute_query`
  (admission, active query, lease renewal), with the result fully
  consumed.
- Its dispatcher has no recovery experiment and none can be attached, so
  it is OFF there and recorded as such. The scheduler path keeps ADR
  626's H 5000 / C 12000.
- Same-path references:
  - **scheduler:** `slow_scan`, `slow_join`;
  - **coordinator:** `slow_scan`, `slow_join`, `hash_join`,
    `grouped_aggregate`, `except_all`, `row_number`, `long_join_last`.

**The frozen schedule** has 43 attempts, plus links before and after:
- 9 references;
- 8 local combinations (both faults, both roles, both paths);
- 18 healthy coordinator-path runs (the six cases x3);
- 8 host losses (the asymmetric matrix).

## Results (reviewed)

Accepted on review as a scoped diagnostic. Every figure below was read
from the campaign's evidence. No tuning, recovery or behaviour change
follows from this closure.

**Identities.**
- **Source:** baseline `5b86af4a` (tree `9237e4d6`), tag
  `adr627-baseline`. It differs from the laddered `4096e788` (tree
  `6965ef5e`) only in this document.
- **Ladder:** `ladder-3` (toolchain 1.95.0, all rc 0).
  - Retained: `ladder-1` with its tpch rerun (its first tpch run
    failed on a cold-generated fixture file, an inferred cause);
    `ladder-2`, superseded by the signal-stamp fix.
  - Development runs are kept separately and are not evidence.
- **Artifacts:** worker `8bc5d5b7…`, probe `dc5a7077…`, identical on
  all three hosts.
- **Freeze and evidence:**
  - `FREEZE.json` `b0779ec6…`;
  - schedule `a3e2bfd9…` (43 attempts);
  - readout `b5583a5f…`;
  - archive of 1534 files with `SHA256SUMS` `b7eed565…`, retained on
    bigfoot under `~/work/toron-evidence/adr627/campaign`, together with
    the raw per-attempt evidence.

**Run.**
- Links before, then the 43 attempts in frozen order, then links after.
- Every attempt returned rc 0, and there was no STOP. Both link
  campaigns measured 48 of 48 runs.
- The readout, recomputed from raw evidence, is READ with no reasons.
  All 9 references completed cleanly.

**Clocks.** Intervals are on the probe's own clock, starting at
`signal_sent`, which is not the confirmed remote signal; the
confirmation latency is separate. The signal landed 3000.936–3002.109
ms after the victim's `StartTask` write. All 16 fault attempts bound as
mid-scan from raw events.

**Scheduler path** (ADR 626's experiment on; 10 attempts):
- **Local:** kill and stop, final-stage and producer.
- **Hosts:** kill of the final-stage task, kill of the producer, and
  stop of the final-stage task, each on strix and on optiplex.
- **The ending:** in every attempt, the victim's dispatch stream ended
  in `read_error` at 302025.194–302146.419 ms. The dispatch returned a
  retryable `TaskFailed` ("query IO error: connection lost") at the
  same moment.
- **The result:** the query then completed at parity, at
  310069.041–310702.278 ms for the final-stage task and
  315116.209–316157.451 ms for the producer.
- **This is far beyond 30 s.** `R` = 30 s belonged to ADR 626's
  between-stage fixture, so it is reported only as a contrast, not as
  this ADR's threshold.
- The endings near 302 s are consistent with ADR 577's 300 s idle
  policy, which is not proof that its timer caused them.
- `SIGSTOP` (suspended execution, socket retained; not an ordinary slow
  task and not a packet-loss test) gave the same shape as `SIGKILL`.
- In the producer cases, a later dial to the victim was abandoned after
  about 5001 ms. That is ADR 626's 5 s establishment bound during the
  replay.

**Coordinator path** (production `DistributedQueryCoordinator`,
experiment off; 6 attempts):
- **Local:** kill and stop, final-stage and producer.
- **Hosts:** kill of the final-stage task, on strix and on optiplex.
- **The ending:** a post-signal dial to the target started at
  1933.387–1997.967 ms and was abandoned at 100807–101978 µs, before
  any frame was written. The victim stream dropped, and the query
  returned a typed `WorkerLost` naming the victim at
  2035.517–2100.112 ms. There was no recovery.
- **The lease reading** is "consistent with lease failure". The
  caller, renewal or issuance, is not proven, and `fail_query_lease`'s
  timing is unavailable.
- **These roughly two-second terminal failures depend on the 5 s
  cadence's phase.** They are not a general end-to-end bound.
- **The host matrix is asymmetric:** producer and `SIGSTOP` coverage on
  this path is local-only, with no cross-host conclusion.

**Healthy coordinator-path controls** (18 attempts, no fault
injected): the central finding.
- **Short cases:** all 12 attempts of `hash_join`,
  `grouped_aggregate`, `except_all` and `row_number` completed at
  parity. They finish before the first 5 s tick, so they do not test
  periodic renewal reliability.
- **Five of the six attempts that outlived one tick returned
  `WorkerLost` with no injected fault:**
  - `slow_scan`-1 and -2;
  - `long_join_last`-1, -2 and -3.

  Their terminals fell at 5.121–5.223 s from the probe log's start,
  around the first expected renewal opportunity. Just before each, the
  observed operations were:
  - a dial to the lost worker abandoned at about 101 ms;
  - or a lease-only stream with no ACK (dial 90060 µs and 99664 µs);
  - or, in `long_join_last`-3, an ACKed renewal candidate to one
    worker, then a dial to the other abandoned at 101501 µs.
- **`slow_scan`-3** completed at parity, with ACKed lease-only streams.
- **The frozen long coverage (at least 5 ACKed lease-only streams to
  each of at least 2 workers) was not met in any long attempt.**
- **For context,** an ordinary issuance dial in `long_join_last`-1 took
  275539 µs on this link.
- **What these runs support:** the observed operations and the code are
  consistent with the current renewal deadlines causing these failures
  on this setup.
  - Those deadlines are 100 ms to connect a fresh connection, then a
    separate 100 ms for the whole `issue_or_renew` operation,
    including its terminal exchange, not only the ACK wait.
  - The caller is not proven, and `fail_query_lease`'s timing is
    unavailable.
  - No general failure rate follows from six attempts.
  - No injected fault is not proof that the network and hosts were
    healthy.

**Successors, in priority order:**
1. **Production lease reliability under slow or transient
   connectivity.** It must consider ownership and namespace expiry, and
   distinguish transient renewal failure from task or worker death.
   Larger deadlines or connection reuse are candidates, not selected
   fixes. No-fault long controls and bounded failure behaviour stay in
   its acceptance contract.
2. **The established-task stall and a watchdog `W`.** It must not turn
   long, silent, healthy execution into worker death without a progress
   contract.
3. **Recovery on the coordinator path.**
4. **ADR 626's deferred dispatch-exclusion shortcut.**

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
