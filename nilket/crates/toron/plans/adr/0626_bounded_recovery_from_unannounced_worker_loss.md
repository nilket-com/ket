# ADR 626 - bounded recovery from unannounced worker loss

**Status:** CLOSED, scoped to the declared Stage C controls and campaign
(see [Stage C results](#stage-c-results-reviewed)). In that campaign,
every unannounced loss recovered at reference parity within `R` = 30 s.
`H` and `C` stay experimental, and `W` stays deferred.

Successor to ADR 625, which closed LIMITED. There, after an unannounced
worker loss, no terminal return or correct completion was observed
within 30 s of the confirmed kill.

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

*Stage A instrument amendments (reviewed).*
- **Where the instrument sits.** All events are observed at the transport
  and stream boundary, in the fixture worker and the probe. No library code
  changes.
  - The observer forwards every byte and error unchanged.
  - A failed event write is reported on stderr as `wait event log write
    failed`, and the readers treat it as incomplete instrumentation.
  - Every event carries the instant it captured (`at_us`). Each interval
    field is an exact difference of two such instants, on one process's
    clock.
- **What is observable, and under which name:**
  - **Establishment:** QUIC connect and stream open, as one combined
    interval. They are not separable here.
  - **Frames:** each frame's header and payload, in both directions. Each
    frame's envelope prefix (protocol version, kind, identity) is decoded
    from its first bytes with the library's own wire types.
  - **Request to first response byte:** from the end of the request frame
    to the first response byte.
  - **Read gaps within a frame:** these include the consumer's own read
    pacing and event-write overhead. They are reported as observed read
    gaps, and they cannot by themselves justify a producer-progress `W`.
  - **Worker task timing:** taken from served-stream boundaries. The
    consuming task of a pull is the served stream's `StartTask`, tagged
    through a task-local where the work stays on that task, and reported as
    unavailable otherwise.
  - **Not observable:** per-pull decode, and task execution itself.
- **Sized fixtures.** The tables have 16, 16 Ki and 192 Ki rows (about
  1.2 KiB, 1.2 MiB and 14.3 MiB raw), each a unique `id` and a 64-byte pad,
  built on first scan only.
  - The test budget caps a frame at 16 MiB, so partition bytes are measured,
    not set to nominal sizes.
  - A join on keys that never match carries the payload across the shuffle
    and returns no rows.
- **Pull-survey coverage.** Each unordered host pair runs 10 queries with a
  two-worker spec. Each pull is tied to its run by:
  - its decoded `PullPartition` address;
  - that run's accepted producer;
  - its `PartitionData` response;
  - its consuming task;
  - byte totals that must cover the task telemetry.

  Complete pulls are reported separately for each of the six directions,
  with `n`. A direction or size with fewer than ten, or a local pull, gives
  no input to a bound.
- **The dispatch-target diagnostic** observes whether a dispatch actually
  targeted the dead worker. If none did, that coverage is reported as
  unavailable.
- **The local diagnostic** runs the staged, hashed release worker and probe.
  Its kill is confirmed by the victim's exit status.
- **Survey lifetimes.** Each survey has a declared budget: every run at its
  own timeout. Worker lifetime covers the budget plus a margin, and a survey
  stops, recorded, before a run could outlast it. A worker that ends on its
  own before cleanup refuses the attempt; only the loss diagnostic's bound
  victim is exempt.
- **The frozen Stage A schedule:**
  - two local diagnostics: producer and dispatch target;
  - three host diagnostics: producer twice, dispatch target once;
  - a handshake survey from each of the three origins;
  - a pull survey at each of the three sizes;
  - a link campaign before and after.

  The readout refuses any other set.

## Stage A results (reviewed)

**Outcome:** an integrity readout of **READ**, with **LIMITED** survey
coverage. READ means every integrity check passed. It does not mean every
survey objective was met.
- **Readout:** `readout-stage-a.json`, sha256 `80da58f44b135cde2207bb838f5406cc10dccb84af7e7145a0a0c88f8396ddbe`.
- **Frozen schedule:** 11 attempts (`schedule.json` `be61b2ea6cd17412b01eb746841d22e1b0fb6d9992e2ee67310e688ec20c9a0d`),
  with link campaigns `a` before and `b` after.
- **Every attempt** finished with verified cleanup and complete evidence. No
  STOP was written.

**Provenance.**
- **Source:** `diag: adr 626 passive wait events`
  `0d9786a1fd6a207166a6e54b174c0aaf0d4e430c`, tree
  `49aaed874414c47d1629acae72316dd633a1cb88`.
- **Ladder:** the retained ladder on that tree is at
  `bigfoot:/home/me/work/toron-evidence/adr626/ladder-diag-2/`:
  - toron: 46 binaries, 5561 passed, 0 failed, 10 ignored;
  - toron-tpch: 674 passed, 0 failed, 6 ignored;
  - fmt clean, and both release builds with 0 warnings. The debug-test
    `adr589_plain_write` warning belongs to ADR 589.
- **Executables:** the worker
  `dcfc1a12b7db1a35ca41fc7db9204a72a2d153c0c9bb58f75884511818081de1` and
  the probe `47fad2548f66b1a9de540c0daa86dd2bb7bcda1223ccf9f63014a9a4ce2015ca`
  were staged per host, with `manifests.json`
  `cb9e2f61c6a78820bbf3823cc5ea605300ddd6458696f069d1c400d2cd9dc059`. Each
  host's manifest was verified on that host.
- **Retained evidence:** `bigfoot:/home/me/work/toron-evidence/adr626/campaign/`,
  holding the copied evidence, the harness, the ADR 625 reader it imports,
  and the source diff. Its `SHA256SUMS` lists 1,540 files and has sha256
  `7cd29f9e729892ce7fb0fcfcfc25d3513e4405e66d34f3f4b32b0c1aef730c78`.
- **Freeze record:** `freeze-stage-a.json`
  `315a77fb2fe26e7daa5e70dfddfdfeab399840af8f584c1a7adb9a4c7bd32a57`.

**Two reader corrections after the data (disclosed).** The first readout
was REFUSED (`readout-stage-a-refused-reader-bug.json`
`d7bf9c80b86795fc3487bb74ccfa8bb8b2615fd07707ab9289ae97171b6fe9cc`, kept).
The evidence did not change. The corrected readers no longer match the
frozen hashes:

| File | Frozen | Corrected |
|---|---|---|
| `adr626_read.py` | `ab349100…` | `c5cf74d1…` |
| `adr626_campaign.py` | `8630a7a2…` | `ecf4d7d1…` |
| `adr626_read_test.py` | `227c8082…` | `a3e3fb4c…` |
| `adr626_campaign_test.py` | `fb6139e7…` | `e0d397a2…` |

1. **Ended-early workers.** The host primitive records `recorded group
   absent` after its own SIGTERM as well. So a worker counts as ended early
   only when the absence is seen before any signal. The fake host's stop
   output now follows the real sequence.
2. **Retried tasks.** A completed run may contain retried tasks. Each task's
   attempts must run from 0 without gaps, with telemetry for the final
   attempt, earlier attempts equal to `retry_count`, and manifests only on
   final attempts. Superseded attempts are reported with their own dispatch
   outcome.

**The located waits.** Loopback and the hosts show the same shape. All
figures are single-clock:
1. **A dial to a dead worker fails at about 300 s.** That holds for both the
   probe's dispatch dial and a consumer's sealed-pull dial. Each stays in
   establishment (connect and stream open together) and fails with `query IO
   error: timed out`:

   | Run | Dispatch dial (µs) | Pull dial (µs) |
   |---|---|---|
   | local producer | 300,001,753 | 300,000,814 |
   | host producer 1 | 300,001,784 | 300,001,410 |
   | host producer 2 | 300,001,791 | 300,001,551 |
   | local dispatch target | 300,001,970 | not applicable |
   | host dispatch target | 300,001,421 | not applicable |

   This is consistent with the configured 300 s QUIC idle policy
   (`TORON_QUIC_MAX_IDLE_TIMEOUT`, ADR 577, `toron_client::liveness`). The
   wrapper does not observe quinn's timer, and does not separate connect from
   `open_bi`. Loopback did not end these dials sooner.
2. **Producer loss ends the query; it does not recover.** The surviving
   consumer reports its pull timeout as `Internal { "query IO error: timed
   out" }`, which is not retryable. `dispatch_loop` returns an error 300.0 to
   300.1 s after the confirmed kill. No producer is invalidated or replayed.
3. **A dead dispatch target is picked again.**
   - The dispatch to the dead worker fails at about 300 s as `TaskFailed {
     retryable: true }`.
   - Its retry completes on a live worker in 0.02 to 0.03 s.
   - Round-robin placement then picks the same dead worker for a later task.
     A second establishment wait was still pending at the 360 s window.

   Nothing excludes a worker after a timed-out dial.

No stall after establishment was observed in these losses. Stage A observed
the live-worker dispatch path on its own runs. The wait of ADR 625's optiplex
task remains uninstrumented and unidentified.

**Healthy surveys.** Quantiles use nearest rank; `n` is given.
- **Handshakes:** 50 per direction, with 0 failures and 0 unavailable
  timings. Maximum establishment (ms) per direction:

  | Direction | Max establishment (ms) |
  |---|---|
  | bigfoot→optiplex | 214.9 |
  | bigfoot→strix | 125.8 |
  | strix→bigfoot | 143.1 |
  | strix→optiplex | 247.7 |
  | optiplex→bigfoot | 305.4 |
  | optiplex→strix | 215.0 |

  Establishment plus the application acknowledgement completed within at
  most 223, 350 and 1,125 ms from the bigfoot, strix and optiplex origins.
- **Pulls, 16- and 16 Ki-row tables:**
  - 20 complete pulls per cross-host direction, with 0 failures and 0
    retries.
  - Establishment at most 293.3 ms.
  - Request written to first response byte at most 114.0 ms.
  - Largest whole-frame read gap 146.9 ms, with no gap of 1 s or more.
  - 309 KB frames took 54 to 334 ms per whole frame.
- **Pulls, 192 Ki-row table:** 18 of 30 queries failed, and 8 completed only
  after a retry. Every failure and retry is the stage-0 producer refusing
  with `BudgetExhausted { ShuffleBytes, requested 3,729,032, available about
  3.36 MB }`.
  - This establishes budget exhaustion. That the partitions retained across
    survey queries by long-lived fixture workers fill the budget is a
    code-supported explanation, not a measured allocation path. These
    refusals are not attributed to Wi-Fi.
  - Complete pulls by direction:
    - bigfoot→strix: 16;
    - strix→bigfoot: 16;
    - bigfoot→optiplex: 8;
    - optiplex→bigfoot: 8;
    - optiplex→strix and strix→optiplex: none.
  - About 3.7 MB frames took at most 1,086 ms per whole frame, with a largest
    in-frame read gap of 187.9 ms.
- **Coverage limits:**
  - In each two-worker spec the first-listed worker produced the payload,
    so large partitions crossed only some directions. The reverse directions
    carried only `customers` partitions under 1 KB.
  - The reader's per-direction `available` label counts samples. It does not
    establish payload-size coverage.
  - Read gaps include the consumer's own pacing and event-write overhead.
- **Links:** both campaigns validate, and the Wi-Fi moved during the run.
  - UDP loss from bigfoot to optiplex, at 100 Mbit/s offered, had a median of
    26.36% in `a` and 63.16% in `b`.
  - Ping maxima were 624 to 629 ms in `a`, and 1,689 ms from optiplex to
    bigfoot in `b`, with 0% ping loss.

**What Stage A decides:**
- **H = 5 s** is approved as a provisional experiment value for
  establishment at the scoped worker operations. It is about 16 times the
  305.4 ms worst observed establishment. It is neither a production default
  nor a bound on future Wi-Fi.
- **W is not approved.** There was no post-establishment loss, payload
  coverage was partial, the read gaps include consumer pacing, and the
  fixture budget interfered. W stays deferred work, needing its own reviewed
  controls and survey.

## Stage B: bounds, recovery, fencing (conditional on Stage A review)

These are constraints the Stage B design must meet. They are not yet an
authorization.

*Stage B amendment after the Stage A review (reviewed; supersedes Parts 2
and 3 where they differ, and Part 4's disposition of unknown or wrong-worker
completions).*

**Scope.** This amendment covers four things only:
- deadlines on establishment;
- typed failure propagation;
- recovery local to the query;
- fencing.

`W` is deferred. Stage A observed no post-establishment loss, and its read
gaps cannot separate producer progress from consumer pacing. A progress
deadline needs its own reviewed controls and survey.

**Declared budgets.** These are provisional experiment values, not production
defaults. Each comes from the declared rule (the larger of a floor and 10
times the worst observed healthy value) over the Stage A survey.

| Name | Bounds | Value | Basis |
|---|---|---|---|
| `H` | establishment (connect plus stream open) | 5 s | max 305.4 ms handshake, max 293.3 ms pull, over 300 handshakes and 240 cross-host pulls (16 and 16 Ki rows) |
| `C` | one confirmation, in total: establishment and the application acknowledgement | 12 s | max 1,125 ms (optiplex origin); 10 times that is 11.25 s, rounded up to 12 s |

`C` is one outer deadline, from the start of the confirmation through its
acknowledgement. If the confirmation's own establishment is also bounded by
`H`, that sits inside `C`. A fresh `C` never starts after `H`.

**Activation.** `H` and `C` are not production defaults. They take effect
only through an explicit, validated experimental configuration:
- it is enabled consistently on the coordinator and on every worker in a
  fixture;
- it is recorded in each attempt's evidence;
- zero, invalid or overflowing values refuse.

Unset, behaviour is unchanged. Keeping or dropping these values is a
declared decision after Stage C.

**The two paths, with their bounds stated separately:**
1. **The dispatch-target path** (the coordinator's own dial):
   - When a task dispatch's establishment exceeds `H`, the result is a typed,
     coordinator-side `WorkerUnreachable { worker, phase: Establishment,
     elapsed }`. The task's attempt fails retryably.
   - The worker is **excluded from this query's placement only**. It is not
     marked lost in the registry, and its accepted manifests are not
     invalidated by this alone.
   - The task is placed again on another worker within its replacement
     limit.
   - Detection is bounded by `H`.
2. **The consumer-report path** (two vantage points):
   - When a consumer's sealed-pull establishment exceeds `H`, the consumer
     reports a typed `ProducerUnreachable`. The report carries the producer
     worker and endpoint, the exact partition address (query, producer stage,
     task and attempt) and the elapsed time.
   - The scheduler then runs **one** confirmation, coalesced per (query,
     worker), bounded by `C`: its own authenticated dial plus a
     `PullPartition` for an impossible partition, which must come back
     `PartitionMissing`.
     - **Acknowledged:** the failure is link-specific. The consumer is retried
       once, on a worker other than the one it failed on. A second such
       failure ends the query with a typed terminal error.
     - **A liveness failure** (the `C` deadline, or a typed liveness error):
       the worker is excluded from the query. Its accepted manifests for
       **this query** are invalidated, query-scoped and without
       `mark_worker_lost`. Their producers replay at their next attempt on
       surviving workers, and their consumers are invalidated and dispatched
       again with the replacement references.
     - **A refusal or protocol error** (a TLS, certificate or version
       refusal, a wrong response, or a malformed or mismatched
       acknowledgement): terminal, with its typed cause. It never leads to
       exclusion or replay.
   - **The confirmation request** uses a fresh query identity and address
     that cannot collide with a live partition. It validates the response's
     identity and accepts only the expected `PartitionMissing` reason.
   - **The confirmation is fenced when it completes, not only when the
     report is accepted.** It carries the accepted producer task, attempt,
     worker and endpoint, and the current consumer identity that justified
     it.
     - Before its outcome is applied, those associations are checked again
       against the query's current state.
     - A late confirmation cannot invalidate replacements, or start a second
       replay wave once another path has resolved the suspicion.
     - Coalescing lasts until its decision is applied, and waiter and
       reservation ownership is released exactly once.
   - Detection is bounded by `H + C` (17 s), plus the consumer's report
     latency.
   - Replayed execution is not bounded: established execution keeps ADR
     577's policy. `R = 30 s` remains this fixture experiment's outcome
     deadline, not a production recovery guarantee.

**Typed classification.**
- **Establishment failures** are typed from quinn's own errors, replacing
  the current blanket `ConnectionRefused` mapping in
  `QuicEndpoint::connect_stream_with_server_name`.
  - The typed cause survives every `std::io::Error` and `QueryError`
    conversion. There is no string matching, and no blanket mapping of an
    unknown error to liveness.
  - Every quinn establishment variant in the pinned version is classified
    explicitly, including a local close or reset versus a peer refusal. An
    unknown variant fails conservatively, as a refusal.
  - Our `H` expiry, or an idle timeout, is **liveness**.
  - A TLS or certificate alert, a version mismatch, or a refused connection
    is a **refusal** (`PeerRefused`).
- **The dispatch-target path's provenance is the coordinator's own bounded
  dial.** A remote wire failure is never taken for a coordinator-generated
  `WorkerUnreachable`.
- **A refusal is never a liveness signal.** It ends the operation with a
  typed terminal error, and never leads to exclusion or replay.
- **Wire compatibility.** The new `TaskFailureKind` variants are appended
  last, and every host runs one build.

**Report binding.** A `ProducerUnreachable` report is accepted only when
both of these hold:
- it is the outcome of the consumer task's **current** attempt, from the
  worker that attempt was dispatched to;
- its address names a **currently accepted** manifest: the same task and
  attempt, the same producer worker, and the same endpoint.

Anything else is handled as follows:
- **A report naming a superseded attempt** is expected and stale. It is
  counted and discarded without mutation, and it spawns nothing. The
  existing invalidation and replacement own the consumer's progress. A
  consumer that is still current is retried only through the validated
  link-specific path, under its one-alternate-worker rule and within the
  limit across all causes.
- **An unknown task or attempt, the wrong worker, or a mismatched producer**
  is a typed `ProtocolViolation`, terminal for the query.

**Query-local exclusion** constrains both the preferred-affinity selection
and the fallback selection. It survives every `dispatch_loop` call for that
query, never affects another query or global registry membership, and is
cleared, with all other query-owned state, when the query is torn down.

**Limits.**
- At most 2 replacement attempts per task, counted across every cause
  (retries and replays).
- At most `workers - 1` workers excluded per query.
- Exceeding either limit, or running out of eligible workers, is a typed
  terminal error. It never hangs.

**Cancellation and ownership.**
- **Every bounded operation is a `tokio::time::timeout` over a future its
  caller owns.** On expiry the future is dropped.
  - Dropping it does not mean the endpoint keeps nothing. In the locked
    quinn 0.11.9, a spawned `ConnectionDriver` continues, and
    `ConnectionRef::drop` starts an implicit close, so protocol teardown may
    go on.
  - Fixtures must show that timed-out connect and open-stream operations
    close or abandon what they own, without accumulating live handshakes or
    streams, while unrelated connections keep working.
  - A shared endpoint is never closed to enforce `H`.
- **Confirmations** are futures in a second `FuturesUnordered` owned by
  `dispatch_loop`. They are dropped when the loop exits, and nothing is
  detached.
- **In-flight dispatches** to an excluded worker end at `H` as their own
  failures, or are abandoned, and their late outcomes are fenced (Part 4).
- **Two properties are proved separately:** that reservations are released
  at scheduler scope, and that remote work on the peer is cancelled or
  fenced. Dropping a local future proves neither.
- **`H` stops at stream open.** An established, healthy, silent execution
  is never bounded by `H`; Stage C's long-silence control checks this.

**Fencing.** Part 4 stands, except that this text supersedes its
disposition of protocol violations:
- one validated transition for completions and failures;
- expected stale or duplicate outcomes are harmless: ignored, with no
  mutation;
- an unknown task or attempt, or a wrong worker, is a typed
  `ProtocolViolation`, raised before any state changes;
- final results owned by attempt;
- every affected consumer fenced, and reservations released exactly once.

**Reported outcomes** stay separate:
- a typed terminal failure;
- bounded detection;
- successful completion within the fixture's `R = 30 s`.

Report latency and replay execution are not bounded by `H + C`.

**Telemetry**, per worker and task:
- `establishment_timed_out_at`;
- `report_accepted_at`;
- `confirmation_started_at` and `confirmation_ended_at` (with the outcome);
- `excluded_at`;
- `replay_dispatched_at`.

**Code scope** (the whole Stage B diff):
- `quic.rs`: typed establishment errors;
- the worker's sealed-pull path: `H` and `ProducerUnreachable`;
- `NetworkTaskDispatcher`: `H` on the dispatch dial, and
  `WorkerUnreachable`;
- the scheduler: query-local exclusion, confirmation, report binding,
  limits, fencing and telemetry;
- `error/distributed.rs`: the new failure kinds.

No TLS change, no change to the ADR 577 policy, and no heartbeat work.

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
- **Stale delivery.** Amended before any Stage C evidence; see
  [Stage C amendment](#stage-c-amendment-stale-coverage). In the
  cleared Stage B design, stale delivery on the wire is not reachable
  through the confirmation path, so this control becomes the coverage
  table below: local cancellation, and unit-only stale fencing.

### Part 6: the three-host campaign

This uses ADR 625's harness discipline, with separate evidence for every
attempt:
- references;
- the healthy controls;
- three unannounced losses of a sealed producer;
- one unannounced loss of a dispatch target;
- one announced-loss regression.

There are no host stale-delivery attempts: see the Stage C amendment.

Each loss attempt reports the detection telemetry against the confirmed
kill, and the outcome (correct recovery, or a typed terminal failure)
against `R`.

### Stage C amendment: stale coverage

Recorded on review, before any Stage C evidence.

**Reachability, stated narrowly.**
- `apply_confirmation` returns the identities it supersedes.
  `dispatch_loop` aborts each one's owned dispatch future through its
  `AbortHandle`, and `Abortable` checks the abort flag before it polls
  the inner future.
- So, on this path, a superseded attempt is retired before it can
  deliver an outcome through the loop.
- This says nothing about every possible path or API. The
  unconditional `OutcomeDisposition` guard stays.

**Coverage.**

| Case | Status | Evidence |
|---|---|---|
| Stale producer success with a sealed manifest and telemetry | Unit-only | `stale_manifests_failures_reports_and_telemetry_change_nothing`; `task_state` `stale_completed_attempt_rejected_after_invalidation`; `outcome_disposition_separates_current_stale_and_violations` |
| Stale failure with telemetry | Unit-only | the same scheduler fixture |
| Stale liveness report | Unit-only: raises no suspicion | the same scheduler fixture |
| Stale final batch | Unit-only: no batch kept | `stale_and_unknown_outcomes_are_fenced_before_any_change` |
| Accepted final result on a lost worker | Demonstrated in-process: replaced without duplicate rows | `an_accepted_final_task_on_the_lost_worker_replays_without_duplicate_rows` |
| Local cancellation and reservation release of superseded attempts | Demonstrated in-process | `superseded_attempts_that_never_return_cannot_fill_a_bounded_frontier` |
| Remote cancellation | **Not provided** | none; a retired attempt keeps running on its worker until it ends |
| Late wire activity from a resumed old attempt | **Not demonstrated** | none; the `SIGSTOP`/`SIGCONT` control is dropped from this record |

What the stale fixtures assert:
- no manifest, final batch, task telemetry, acceptance state, retry or
  suspicion changes;
- the only change is the declared `stale_rejection_count` and one
  `stale_outcome` event.

**Preliminary observation (local relay control; not campaign evidence).**
- In the past-`H` relay control, the coordinator's own dispatch to the
  blackholed worker timed out after 5030.4 ms. That excluded the worker
  directly, without a confirmation, and kept its manifests (the
  dispatch-target path).
- A consumer then reported the same worker's sealed producer. That
  report still waited the full `C` before the confirmation ended at
  17036.1 ms and the producer was replayed. The control recovered at
  parity 17077 ms after the cut, within `R`.
- Treating a dispatch-only exclusion as confirmation for later reports
  would merge the two paths that Stage B keeps distinct. That shortcut
  is deferred to a successor plan, and Stage C does not change it.

**Claims.**
- No claim of remote cancellation, of a resumed old worker's behaviour,
  or of late-wire fencing.
- Retirement is reported as local cancellation.
- `W` stays deferred.

## Stage C results (reviewed)

Accepted on review, within the declared experiment. Every figure below
was read from the campaign's evidence.

**Identities.**
- **Source:** baseline `11f58d8a` (tree `e182e09f`), tag
  `adr626-stage-c-baseline`, after the plan amendment `3df3d346`. The
  baseline differs from the laddered `fcf67a40` (tree `00700228`) only
  in this document.
- **Ladder:** `ladder-stage-c-4` plus `tpch-full-rerun-2` (toolchain
  1.95.0), as one composite record.
  - In that ladder, toron, client, fmt and both release builds returned
    rc 0. The first toron-tpch run returned 101 (673 passed, 1 failed),
    and full rerun 2 returned 0 (674 passed).
  - The failed test read a strict SF1 parquet file that was written
    during the same step in a fresh worktree. That cause is an
    inference, not an established fact.
  - Retained records: `ladder-stage-c-2` is INVALID (its tree was reset
    mid-run); `ladder-stage-c-3` ran on toolchain 1.98.1 (NOTE); one
    full tpch rerun ran without agent forwarding (INVALID).
- **Artifacts:** `toron_fixture_worker` `d699db5d…`, probe `1927db68…`,
  identical on all three hosts. The per-host manifests were verified on
  each host.
- **Freeze and evidence:**
  - frozen in `FREEZE.json` `f206ae4e…`;
  - schedule `eb660f88…` (29 attempts);
  - readout `b9331daf…`;
  - evidence archive of 1003 files with `SHA256SUMS` `9b7ec000…`,
    retained on bigfoot under
    `~/work/toron-evidence/adr626c/campaign`.

**Run.**
- Links before, then the 29 attempts in frozen order, then links after.
- Every attempt returned rc 0, and there was no STOP. Both link
  campaigns measured 48 of 48 runs.
- The readout, recomputed from the copied raw evidence, is READ with no
  reasons.

**Controls.**
- **References:** 5 of 5 completed with zero recovery events.
- **Healthy controls:** 15 of 15 passed (`hash_join`,
  `grouped_aggregate`, `except_all`, `row_number` and `slow_scan`,
  three each, at `H` 5000 ms and `C` 12000 ms). Each was at reference
  parity, with zero recovery events, every attempt 0 and no retries.
  There were no false suspicions in these controls; nothing is claimed
  about Wi-Fi in general.
- **Local controls** (staged release executables, separate from the
  preliminary runs):
  - Relay below `H`: establishment 604 ms, query 7081 ms, zero events.
  - Relay past `H`: recovered at parity 17072 ms after the cut.
    Coalescing was observed, recomputed from raw events: one
    confirmation interval with two reports.
  - Long silence: 8024 ms, longer than the local `H`, with zero events.
  - Relay lifecycle: exact test proof.
- **Announced-loss regression** (experiment off): passed, with zero
  recovery events.

**Unannounced loss (experiment on).** All four attempts were classified
as recovered, at reference parity, with event identities bound to the
final run and no false exclusions.
- The three producer losses are repetitions: each killed the same worker
  (strix, 70202). The dispatch-target loss killed optiplex (70203).
  Losses on every host and in every role were not covered.
- All times are scheduler-clock deltas from the kill marker on the
  coordinator (bigfoot), except completion, which is measured from the
  confirmed kill.

| Attempt | Completion | Dispatch timeout and exclusion | Consumer replay | Liveness decision | Producer replay |
|---|---|---|---|---|---|
| producer 1 | 17698 ms | 5097.064 ms | 5097.081 ms | 17101.324 ms | 17101.338 ms |
| producer 2 | 17302 ms | 5069.221 ms | 5069.236 ms | 17070.864 ms | 17070.883 ms |
| producer 3 | 17587 ms | 5070.554 ms | 5070.591 ms | 17072.603 ms | 17072.644 ms |
| dispatch target | 5980 ms | 5000.669 ms | 5000.679 ms | none | none |

- In each producer loss, the coordinator's dispatch to the dead worker
  timed out near `H`. That excluded the worker and replaced the
  consumer. A consumer's report against the sealed producer then waited
  the full `C` before the producer's replacement attempt was replayed.
- That extra `C` wait is the deferred shortcut recorded in the Stage C
  amendment. It is a successor-plan candidate: count a dispatch-path
  exclusion as confirmation for later reports in the same query. This
  closure changes no behaviour.

**Limits.**
- The detection deltas are single-host scheduler-clock steps. They do
  not establish an end-to-end guarantee.
- This is one campaign on one Wi-Fi setup: three producer-loss
  repetitions against one worker, one dispatch-target loss, and 15
  healthy controls.
- `H` and `C` stay experimental, and `W` stays deferred.
- Stale coverage is local cancellation or unit-only, per the amendment.
  No remote cancellation and no wire fencing are claimed.

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
    declared outcomes in the relay controls. Stale fencing is reported
    per the amendment's coverage table, as cancellation or unit-only
    and never as wire fencing.
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
