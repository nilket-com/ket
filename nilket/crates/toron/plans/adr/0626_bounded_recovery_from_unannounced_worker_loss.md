# ADR 626 - bounded recovery from unannounced worker loss

**Status:** PROPOSED, staged. Successor to ADR 625, which closed LIMITED.
After an unannounced worker loss, no terminal return or correct
completion was observed within 30 s of the confirmed kill.

This plan has three stages, and approval does not carry forward:
- **Stage A (Part 1), the only part authorized by this plan's commit:**
  diagnose the wait.
- **Stage B (Parts 2–4):** bounds, recovery and fencing. It is
  conditional. Its numbers, scope and mechanism are reviewed against
  Stage A's readout before any of its code is written.
- **Stage C (Parts 5–6):** controls and the campaign. They are
  conditional on Stage B.

**A correctness and robustness record. Wi-Fi is a supported target, and
no performance claim is made.**

## Context

**What ADR 625 observed** (`loss-unannounced-2`, from its retained
evidence):
- The victim was 70202 on strix, the hash-join stage-0 producer. It was
  bound before the kill to its accepted sealed manifest. The kill was
  confirmed at unix ms …169142.
- After the kill, the probe (192.168.0.2:59157) dispatched to optiplex
  (`STREAM_ACCEPTED` …169445) and to bigfoot (…169636).
- Bigfoot's worker then recorded a sealed-pull dial to
  `70202 192.168.0.8:7701`. The dial record is written *before* the
  connect (`DialRecordingTransport::connect_peer`), so it shows an
  attempt, not a connection.
- Optiplex recorded no dial; what its task waited on is not established.
- `dispatch_loop` returned nothing before the bound
  (`attempt_elapsed_ms` 30826).

**The hypothesis from the code.** Stage A confirms or replaces it.
1. **The sealed pull has no deadline of its own.**
   `ToronWorker::pull_shuffle_inputs` calls `peer_transport.connect_peer`.
   From there, `QuicEndpoint::connect_stream_with_server_name` awaits
   quinn's `connect(..)` and `open_bi()`, and
   `pull_partition_from_worker` reads the response.
2. **The only transport bound is long by design.** ADR 577's policy
   (`toron_client::liveness`) sets a 300 s `TORON_QUIC_MAX_IDLE_TIMEOUT`
   and a 5 s keep-alive on every Toron endpoint. This is right for an
   established, silent, long execution. Whether, and when, it ends a
   handshake or pull toward a vanished peer is **not assumed**: an idle
   timeout is not an end-to-end guarantee.
3. **The scheduler waits on whatever the consumer waits on.**
   `dispatch_loop` advances only when a dispatch future resolves
   (`in_flight.next().await`). Its own link to the surviving consumer
   is healthy and kept alive.
4. **No consumer failure reaches producer replay.**
   `accept_dispatch_failure` retries the *consumer* (when
   `failure_retryable`), with the same producer reference, or fails the
   task terminally. Nothing reaches `mark_worker_lost` without an
   announcement.
5. **No liveness input reaches the loop.** The wire `Heartbeat`,
   `WorkerRegistry::heartbeat` and `expire_worker_heartbeats` exist, but
   no runtime path sends heartbeats or runs the expiry; only a scheduler
   test does.
6. **A dispatch whose target is the dead worker** dials it with the same
   exposure.

**Fencing as it stands:**
- **Task identities carry `attempt_id`.** Invalidation opens a
  replacement attempt without rewriting the completed attempt's history.
- **Partition stores answer another attempt's address** with
  `PartitionMissing(AttemptStale)`.
- **The registry does not select lost workers**
  (`heartbeat_expiry_marks_suspect_then_lost_without_selecting_lost_workers`).
- **Defect: a stale completion changes state, then aborts the query.**
  - `accept_dispatch_completion` calls `set_attempt_state(identity,
    Completed)` and then `accept_completed`.
  - `accept_completed` *requires* `Completed` with the matching worker
    (otherwise `AttemptNotCompleted`). So the mutation is load-bearing,
    and simply reordering cannot fix it.
  - A `StaleAttempt` refusal then propagates with `?` as
    `SchedulerError::Acceptance`, so one late completion of an
    invalidated attempt aborts the whole query.
- **Defect: a stale failure is not checked either.**
  `accept_dispatch_failure` can spawn a retry, or call `fail_task`,
  without checking that the failing attempt is the current one.

## Stage A: diagnosis (authorized by this plan's commit)

### Part 1: locate the waits and survey the healthy timings

**The baseline is immutable.**
- One source commit, `diag: adr 626 passive wait events`, adds
  test-only, passive events and changes nothing else in behaviour.
- It is laddered on bigfoot, and its fixture worker and probe are built
  once, hashed, staged and manifest-verified per host, as in ADR 625.
- All Stage A runs use exactly these executables.

**The passive events,** each with unix ms and single-clock elapsed
times:
- **Consumer sealed pull:** dial start; connect resolved or failed;
  stream open; request sent; first response byte; frame-header complete
  and payload complete per frame (with byte counts); pull resolved or
  failed; decode start and end.
- **Worker task:** received, started, finished, outcome sent.
- **Probe dispatch:** per task attempt, dispatch start, connect
  resolved, response received and returned, with the target worker.
- The existing dial record stays as it is.

**Sized fixtures, so `W` has something to measure:**
- A test-only sized fixture table gives sealed partitions of about 1 KiB
  (today's), 1 MiB and 16 MiB.
- The survey records partition bytes, frame count and maximum frame
  size, so first-response and inter-frame gaps are measured at sizes,
  not inferred.

**The local reproduction,** with process tests on loopback:
- three fixture workers; hash_join; the producer is `SIGKILL`ed after
  its sealed manifest is accepted;
- a 360 s observation window, with the whole test bounded at 420 s;
- the reproduction reports which future is pending, and how and when
  each wait ended, if it did. That includes any ICMP-driven early
  failure on loopback.

**The host runs.** ADR 625's harness is reused, with its per-attempt
authenticated gates, immutable raw evidence, exact reader contracts,
verified process cleanup and LIMITED/REFUSED distinctions.
- **Diagnostic unannounced loss:** two attempts with a remote producer
  victim; one attempt killing a worker before its task is dispatched.
  Each has a 360 s observation window and a 420 s whole-attempt bound,
  with worker and process cleanup bounded as in ADR 625.
- **The healthy survey,** with no kill:
  - 50 authenticated handshakes per directed pair;
  - sealed pulls at each fixture size, 10 per directed pair, with full
    phase timing;
  - a link campaign beside it.

  The survey reports per pair and size: min, median, p99 and max for
  establishment, first response and inter-frame gap.
  - **Sample counts and the quantile convention** are reported beside
    them. With 10 pulls per pair and size, p99 is a descriptive sample
    statistic, not an established tail bound.
  - **Failed or incomplete observations are kept and reported.** A phase
    that could not be observed is named as unavailable. It never becomes
    a zero timing, and it never drops out of the healthy survey without
    disclosure.
- The diagnostic loss attempts are diagnostics; they have no pass
  bound. The readout states what each wait was, and whether and when it
  ended.

**The Stage A deliverable, before any Stage B code:** a readout
covering
- the located waits;
- the survey;
- proposed numeric `H` and `W`;
- the precise progress boundary;
- the scoped mechanism.

It goes to review. If the observations contradict the hypothesis,
Stage B is re-planned.

## Stage B: bounds, recovery, fencing (conditional on Stage A review)

These are constraints the Stage B design must meet. They are not yet an
authorization.

### Part 2: bounded detection

- **Scope.** Bounds apply at the worker-operation boundary: sealed pull,
  task dispatch and coordinator confirmation. They do not apply to
  every `connect_peer` caller. ADR 577's established-connection policy
  is unchanged, and an established, healthy, silent execution longer
  than `H` and `W` must survive (Part 5).
- **Establishment `H`** covers connection establishment and stream open
  for those three operations.
- **Progress `W`** applies to the sealed pull only, at a byte and frame
  boundary that Stage A fixes. The design must state how partial headers
  and payloads, the maximum frame size, trickling bytes, and the
  receiver's own decode and backpressure interact with `W`. Consumer CPU
  time is never counted as producer unreachability. Tests must cover:
  - a large, slow transfer whose total time exceeds `W` while its
    progress stays healthy;
  - a transfer stalled mid-frame.
- **Numbers.** The provisional experiment rule is the larger of 5 s or
  10 × the worst healthy value Stage A observed, for each of `H` and
  `W`. It is **not** an approved production default. The values are
  validated configuration, applied consistently to all three operations.
- **Typed failures.** Liveness timeouts (`H`/`W` expiry) are distinct
  from TLS, authentication or protocol refusals; a refusal is never
  treated as loss. A consumer's timeout yields `ProducerUnreachable`,
  carrying:
  - the producer's identity and endpoint;
  - the exact partition address (task key and attempt);
  - the phase;
  - the elapsed time.

### Part 3: confirmation and recovery

- **What is concluded.** A timed-out consumer report plus a failed
  coordinator probe within the declared bounds means **"unreachable from
  the consumer and the coordinator within the bounds"**. It is not
  proof of death, nor proof that the congestion is not transient.
- **The recovery action is query-local exclusion plus attempt
  fencing.** The worker is excluded from this query's placement, and its
  affected attempts are invalidated and replayed. Global membership is
  not permanently marked dead on this inference.
- **Reports are validated and fenced.**
  - **Identity:** a report is accepted only from the authenticated
    consumer that holds the current attempt of the reporting task. It
    must name the exact currently accepted producer task, attempt and
    endpoint.
  - **Stale reports:** a stale or fabricated report cannot invalidate a
    live replacement.
  - **One suspicion per worker:** simultaneous suspicions of one worker
    are coalesced into one confirmation.
- **When the coordinator reaches the producer,** the failure is
  link-specific. The consumer is retried at most once, placed on a
  different worker. If that also fails, the query ends with a typed
  terminal error. The same path is never retried indefinitely.
- **Explicit limits:**
  - at most 2 replacement attempts per task;
  - at most `workers - 1` workers excluded per query.
- **One recovery budget.** All confirmation and replay-dispatch work
  draws on a single budget, measured on one clock. The detection path's
  bound is stated per path: `H` alone (dispatch), `H` then confirmation
  (an establishment failure), or `W` then confirmation (a progress
  failure). Confirmation is one budgeted step, not two separately timed
  substeps.
- **`R` is this fixture experiment's deadline,** proposed at 30 s from
  the confirmed kill to a terminal return. Replayed execution has no
  execution deadline under ADR 577, so no universal production bound is
  claimed. A production recovery budget, with a cancellation contract,
  would be a separate proposal.
- **Two different outcomes are reported:** correct recovery, and a
  bounded, typed terminal failure.
- **Detection telemetry:** `suspected_at`, `confirmed_unreachable_at`,
  `excluded_at` and `replay_dispatched_at`, per worker and task.

### Part 4: fencing

- **One validated transition.** Completion acceptance becomes a single
  atomic transition: either a validation with no side effects and the
  existing transition under exclusive ownership, or one combined
  operation. Every refused completion leaves state untouched. The
  refusals fall into two classes:
  - **Expected stale or duplicate outcomes:** a completion of an
    invalidated or superseded attempt, or a repeat. These are counted
    and discarded, and the query continues.
  - **Protocol violations:** an unknown task or attempt, or a completion
    from the wrong worker. These are **not** preapproved as harmless.
    The Stage B proposal gives each a typed disposition, and explains
    why it cannot mask a broken association or leave the query waiting
    indefinitely.

  Every other acceptance error stays fatal.
- **Stale failures are fenced the same way.** A failure from a
  non-current attempt cannot spawn a retry or call `fail_task`.
- **Final results have one owner.** Final-stage batches are held per
  final task, owned by that task's accepted attempt. They are committed
  to the query result only at completion. Invalidating an
  earlier-accepted final attempt drops its batches, and its replacement
  supplies them.
- **Affected consumers are fenced, not just the excluded worker.** Every
  consumer future depending on an invalidated producer attempt is
  cancelled or fenced, wherever it runs. Unrelated tasks are preserved.
  Dispatch reservations are released exactly once.
- **Coverage:**
  - stale success and stale failure;
  - stale manifest, stale final batches and stale telemetry;
  - duplicate completion;
  - dependency invalidation;
  - downstream consumers in flight, and already completed;
  - removal and replacement in `accepted_manifests` and in the final
    result;
  - a stale or fabricated `ProducerUnreachable` report.

## Stage C: controls and campaign (conditional on Stage B)

### Part 5: controls

- **The healthy Wi-Fi control.** The four cases run three attempts each
  with no kill, at the frozen values. Each attempt must show zero
  suspicions, zero exclusions and zero replays, and parity with the
  reference; a link campaign runs beside it. The claim is "no false
  suspicions in the declared controls". Nothing is claimed about Wi-Fi
  in general.
- **The relay control.** A test-only, unprivileged UDP relay:
  - **Frozen settings:** its delay, loss and seed are frozen in advance,
    and the actual handshake and progress durations it produces are
    measured and reported.
  - **Healthy setting:** below `H`, the relay produces no suspicion, and
    the query matches the reference.
  - **Failing setting:**
    - it affects **both** the consumer path and the coordinator's
      confirmation path;
    - the expected result is one coalesced worker suspicion and the
      declared task outcomes;
    - "exactly one suspicion" is not asserted under retransmission and
      concurrency.
- **A long, silent, healthy execution** that outlasts `H` and `W`
  completes, which shows ADR 577 is intact.
- **Stale delivery on the wire.** Test-only barriers make an old
  attempt's outcome reach the scheduler *after* invalidation, while the
  query is still running.
  - This is covered for a producer and for a final-stage task carrying
    batches.
  - The test asserts the rejection event and the reference digest.
  - Where the abandoned future is dropped and cannot deliver, the record
    reports cancellation, not on-wire stale fencing.
  - Where needed, a resumed stale task is injected explicitly
    (`SIGSTOP` and `SIGCONT` around a barrier, with the query still
    active).

### Part 6: the three-host campaign

This uses ADR 625's harness discipline, with separate evidence for every
attempt:
- references;
- the healthy controls;
- three unannounced losses of a sealed producer;
- one unannounced loss of a dispatch target;
- the stale-delivery attempts;
- one announced-loss regression.

Each loss attempt reports the detection telemetry against the confirmed
kill, and the outcome (correct recovery, or a typed terminal failure)
against `R`.

## Acceptance

- **Stage A:**
  - the baseline commit is laddered, with logs retained;
  - the executables are hashed and staged;
  - every diagnostic and survey attempt has complete, verified evidence
    and cleanup;
  - the readout states the waits and the per-pair and per-size
    distributions;
  - the Stage B proposal is reviewed.
- **Stage B:** the full ladder, with logs retained, and the fixtures
  above.
- **Stage C:**
  - **Controls:** zero false suspicions in the healthy controls, and the
    declared outcomes in the relay and stale-delivery controls.
  - **Unannounced loss:** each attempt either recovers correctly within
    `R` at parity, or is reported against `R` and not counted as a pass.

## Guardrails

- **ADR 577's established-connection policy and all statement or
  execution timeouts are unchanged.**
- **No TLS, certificate or host-key change.**
- **Bounds follow the declared rule, and are frozen before use.** They
  are never tuned to pass an attempt.
- **Stage B and C need review before they start.** Stage A's commit
  authorizes no behaviour change.
- **Single-host clocks only.** There are no cross-host timing claims and
  no new heartbeat architecture.

## Risks

- **Wi-Fi congestion may look like unreachability within `H` or `W`.**
  The confirmation, the controls and the query-local scope limit the
  damage. A false exclusion in a control stops the campaign.
- **Quinn's handshake retransmission and ICMP behaviour** differ between
  loopback and hosts. Stage A measures both.
- **Optiplex's post-kill task** may be a second wait. If so, Stage A
  names it.
