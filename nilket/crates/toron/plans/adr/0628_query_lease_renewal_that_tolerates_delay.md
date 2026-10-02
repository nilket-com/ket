# ADR 628 - query-lease renewal that tolerates delay

**Status:** PROPOSED, staged. Successor to ADR 627, which closed with
limited lease coverage.

Approval does not carry forward:
- **Stage A (characterization and pinning)** is the only stage this
  plan's commit authorizes.
- **Stage B (mechanism)** and **Stage C (controls and campaign)** are
  each reviewed before they start; no mechanism is selected here.

**A correctness and robustness record. Wi-Fi is a supported target. No
performance claim is made.**

## The question

How should the coordinator tolerate a *delayed* query-lease renewal,
while still:
- ending its own reliance on a worker's namespace at a declared,
  conservative boundary;
- leaving remote reclamation, and the denial of delayed writes, to work
  as designed?

ADR 628 improves transient-renewal tolerance and fails the query safely
at that boundary. Coordinator recovery (excluding a worker and
continuing) is **out of scope**; it is a separate successor with full
state-fencing semantics.

## Context

### What ADR 627 observed (on this setup only)

- 5 of 6 intentionally long, no-fault production-path queries returned
  `WorkerLost` at 5.121–5.223 s, around the first renewal opportunity.
- The operations observed just before were:
  - a dial abandoned at about 101 ms;
  - or a lease-only stream with no ACK.
- An ordinary dispatch dial on that link took 275539 µs.
- The observations and the code are *consistent with* the renewal
  deadlines causing these failures. The caller is not proven, and no
  rate follows.

### Code-established facts (cited)

1. **Term.**
   - 35.001 s = worker-loss window (30 s) + cadence (5 s) + 1 ms
     (`namespace_lifecycle.rs:24-37`); `new()` rejects a shorter term.
   - Both sides compute it independently from `DEFAULT_WORKER_LOST_AFTER`.
   - Pinned by `lease_term_must_exceed_loss_window_plus_renewal`.
2. **What a lease is, and what it gates.**
   - A worker-local `Lease { deadline, first_seen }` on the worker's
     `Instant`.
   - It gates partition-store writes (`authorize_write`: publish,
     open_live, append_live, seal_live). A pull checks the lease only
     when its partition does not exist.
   - It does **not** gate `StartTask` or task computation.
3. **Renewal on the worker** (`issue_or_renew`, `:230-247`):
   - a no-op while the query is retiring, or while a receipt remains;
   - otherwise the deadline is set to now + term. This includes an
     existing lease whose deadline has **already passed**, so expiry
     alone is not terminal.
   - The server replies `QueryLeaseAccepted` after the call in **every**
     case, including the silent no-op. **An ACK does not distinguish
     applied from ignored.**
4. **Expired write** (`authorize_write`, `:249-263`): removes an
   expired lease and returns `Expired`.
5. **The reaper** ("expiry is abandonment" holds only on this path, and
   only while the expired lease is still discoverable in the lease map):
   - `expired_queries` (`:363-375`) enumerates and removes expired
     leases;
   - the store then calls `retire_query(Abandoned)` *outside* that
     lifecycle lock;
   - `finish_removal` (`:323-347`) issues a receipt only when namespace
     evidence licenses one.
6. **Retirement** (`partition_store.rs:547-572`):
   - cancels the query's live partitions, wakes waiters, and removes
     manifests, partitions and reservations once quiescent;
   - does not touch running tasks.
7. **Receipts:** capacity 4096, horizon 15 min.
   - During retirement, the retiring map blocks renewal (fact 3).
   - After retirement completes, a held receipt is the retained guard
     against re-leasing the query id.
   - That protection is bounded and conditional: a retirement with no
     namespace may leave no receipt. After eviction, or when
   no receipt was licensed (a lease-only or final-result worker), the
   same id can be leased again.
8. **The coordinator renewal loop** (`coordinator.rs:1860-1911`):
   - first tick at now + 5 s;
   - sequential over a growing census;
   - each renewal is a fresh connect within 100 ms, then the whole
     operation (send, ACK read, `finish_terminal_exchange`) within
     100 ms;
   - **one failure** calls `fail_query_lease` and ends the loop.
     Dispatch executions that observe the stop surface `WorkerLost`,
     even on healthy workers. A dispatch caught by the early
     `is_cancelled` check returns `Cancelled`, and one that has already
     completed is unaffected. The pins record actual outcomes.
   - Retirement fanout still runs: retried every 1 s until its own
     spawn + 35.001 s. That is an observed retry horizon, not proof that
     all remote state or tasks are gone by then.
   - No remote `CancelTask` is sent on lease failure.
   - Only a renewal failing by connect error is pinned
     (`failed_query_lease_renewal_stops_reliance_with_worker_identity`).
9. **Per-dispatch issuance:** `QueryLease` then `QueryLeaseAccepted` on
   the dispatch stream, before `StartTask` (`server/mod.rs:5969-5993`).
   No separate timeout applies.
10. **Connections:** every `connect_worker` is a new QUIC connection,
    including the TLS handshake. There is no reuse.
11. **Intent (ADR 543):**
    - "If a lease could expire faster than failover classifies a
      worker, a network partition would reclaim state the coordinator's
      recovery still needs."
    - "The lease is recovery from a lost signal, never the correctness
      trigger."

### Race candidates (code-supported; NOT established leaks or failures)

- **(a) Expired write, then the reaper, with no renewal in between.**
  `authorize_write` removes the expired lease, then `expired_queries`
  no longer finds the id. Does existing namespace state remain
  unreclaimed?
- **(b) The reaper versus a renewal.** `expired_queries` removes an
  expired lease, and a renewal arrives before `begin_retirement`. Does
  that renewal's ACK precede a later reclamation of the namespace it
  appeared to renew?
- **(c) Re-lease of a retired id:** after receipt eviction, or with no
  licensed receipt.

### The coordinator's conservative boundary (conditional, not proven)

A candidate rule. Stage A establishes whether today's evidence and wire
semantics can support it.
- **Assumptions:**
  - The worker's monotonic elapsed time runs at most (1 + ρ) times the
    coordinator's.
  - An operation's exact send marker, on the coordinator's monotonic
    clock, precedes the worker's processing.
- **Rule:** if both hold, a lease that the worker *actually applied* to
  the *same still-valid namespace* does not **expire** before
  L / (1 + ρ) of coordinator time from that send marker, minus a
  declared scheduling/safety margin. L is the term.
  - This bounds deadline expiry only. It cannot exclude an earlier
    explicit retirement, a worker restart, or any other invalidation of
    the namespace.
- **The ACK does not certify the rule's preconditions today** (fact 3).
  It certifies neither that the renewal was applied, nor the worker
  incarnation, nor that no retirement or restart intervened. Therefore:
  - The coordinator must not refresh its boundary from an ignored or
    late ACK.
  - It must not bridge a lapse silently.
  - It must not assume a re-leased id kept its partitions.
  - Initial issuance and every renewal each need their own exact send
    marker. ACK-receive time is not interchangeable with send time.
- **Each assumption must be stated quantitatively, with its limits:**
  - ρ, with its justification;
  - monotonic-clock behaviour across suspend and restart;
  - behaviour when the coordinator itself is stalled at the deadline
    check.
  - Without these, the record states conditional safety, not a
    universal bound.
- **Wire changes:** if the policy needs one (for example, an ACK that
  distinguishes applied from ignored, or an incarnation id), that change
  gets its own Stage B review.

### Open questions

1. Races (a), (b) and (c) above.
2. Remote running tasks after a lease failure: no `CancelTask` is sent,
   and `RetireQuery` does not touch active tasks. What actually stops
   them?
3. Whether the retirement retry horizon (from its spawn, not from the
   last renewal) is misaligned with the worker's lease.
4. Term mismatch is a protocol error. Its wire manifestation (code
   reading suggests an unanswered drop) must be observed per transport,
   not assumed.
5. Missed ticks (tokio's default burst behaviour) combined with
   sequential renewal: does a slow worker push others toward their
   deadline? Named; tested only through the coordinator pins below.
6. **Backend coverage.**
   - Stage A tests the managed in-memory store, which is the production
     worker's store (`daemon.rs:245-250`), and the hybrid store's
     single-receipt path where it already has fixtures.
   - The spill backend's expiry and retirement path is listed as
     unavailable in this record.

## Stage A: characterize and pin (authorized by this plan's commit)

There is no behaviour change. The work is test-only instrument, pinning
tests and a small diagnostic. Current behaviour is recorded as
diagnostic assertions. Any failure found comes back for a reviewed
scope decision before Stage B, never an unplanned worker semantic
change.

### 1. Lifecycle pins

These use managed stores, with deterministic, explicit lifecycle times
and barriers. Unmanaged fixture stores cannot establish lease safety.
- Renewal while retiring, or while a receipt is held: ACKed, no-op, and
  the write is denied.
- Renewal of an expired lease *before* retirement begins: the lease is
  extended or recreated (current behaviour).
- Re-lease after receipt eviction (capacity and horizon), and on a
  worker with no licensed receipt: a fresh lease, and the write is
  allowed.
- Race schedule (a): an expired `authorize_write`, then the reaper,
  with no renewal in between.
- Race schedule (b): `expired_queries` enumerates and removes an expired
  lease, a renewal arrives, then `begin_retirement` runs. Barriers are
  placed between the steps, so that the ACK's order relative to the
  reclamation is observed.
- Race schedules and the re-lease cases stay *code-derived candidates*
  until these deterministic tests run.
- `QueryLeaseExpired` on a write between the deadline and the reaper
  tick.

### 2. Coordinator pins

- A renewal failing by connect timeout, and by operation timeout.
- Renewal failure → in-flight dispatch `WorkerLost`, and a later
  dispatch `Cancelled`.
- No `CancelTask` on lease failure; what actually happens to a remote
  running task.
- Each renewal's exact send marker, outcome, and censored elapsed time.

### 3. Fault controls

Each is frozen before use. Each records the realized phase and fault
events and counters, never a promise of a reproducible packet schedule.
- **Connect-phase delay:**
  - a named transport-future delay fixture that delays dial resolution;
  - distinguished from network-level handshake delay through the relay;
  - stating exactly which timing each changes.
- **Operation-phase delay:** separately delaying the lease write, the
  ACK, and terminal completion.
- **Coordinator-only asymmetry:** independently addressed relay routes,
  so that producer and consumer traffic is demonstrably unaffected.
- **Seeded loss on a declared route.**
- **A blackout shorter than the term, then recovery.**
- **Loss of reachability (a permanent blackout):** reported as
  unreachability, not death.
- **Owner disappearance:** an actual coordinator (owner) termination
  with the worker left alive, so worker-side expiry, reclamation and
  write denial can be observed.
- **Worker kill:** a distinct fault case. It cannot substitute for the
  owner-disappearance cleanup evidence.

The exact subset of these controls is frozen in instrument review.

### 4. Wi-Fi diagnostic (small)

- A small repeat of ADR 627's long production-path no-fault controls,
  to bind the new instrument and today's link coordinate. Sample counts
  and the schedule are fixed in instrument review.
- Durations are **censored** by today's 100 ms caps. An abandoned dial
  or operation reports its elapsed time to cancel, and its censored
  status. That is not a successful renewal latency, and no
  percentile or timeout recommendation is drawn from it.
- Any longer-budget survey of operation durations is a separately
  declared diagnostic, never a quiet change to production deadlines.
- Ticks, caller and the complete census remain unobserved unless a
  passive instrument change is explicitly reviewed.

**Deliverable:** a readout that separates:
- pinned current behaviour;
- race-schedule outcomes;
- fault-control outcomes;
- missed coverage and censored measurements, as named outcomes;
- whether today's ACK and evidence semantics can support the
  conservative boundary.

It goes to review before Stage B.

## Stage B: mechanism (conditional; not selected)

**Candidates:**
- retrying within the conservative boundary;
- deadlines informed by *uncensored* diagnostics;
- per-worker independent renewal, with an explicitly declared
  whole-query policy on any lapse;
- connection reuse;
- any wire or worker evidence change (applied-versus-ignored ACK,
  incarnation), with its own review.

**On a lapse, the query fails safely at the declared boundary.** It does
not exclude the worker and recover.

## Stage C: acceptance (conditional)

These are named separately, never conflated:
1. **Bounded cessation of coordinator reliance** at the declared
   conservative boundary, under the stated assumptions.
2. **Remote reclamation:** expiry or retirement receipts, and delayed
   writes denied, shown explicitly, including after owner disappearance.
3. **Running-task cessation:** reported as observed. The lease does not
   gate computation, so no claim follows from it.

**Also required:**
- No-fault long controls on this Wi-Fi complete at parity over a frozen
  count. A failure is reported, never retried away.
- The declared fault controls behave as declared.
- ADR 627's coordinator mid-task cases are rerun against the boundary.
- No claim is made that a dead worker's memory is remotely cleaned, or
  that a suspended or disconnected owner guarantees its tasks stop.

## Guardrails

- No change to the lease term, the reaper, receipts, TLS or ADR 577's
  idle policy without its own review.
- Single-host monotonic durations only. Clock-rate assumptions are
  quantitative and stated.
- No-fault long controls stay in every acceptance contract.
- Coordinator recovery, `W` and the dispatch-exclusion shortcut are
  separate plans.

### Stage A instrument amendment (reviewed)

Recorded before any frozen evidence. Test-only: no production source
changes behaviour. Every new hook lives in tests, in `tests/support`, or
in the fixture worker binary. Stage B and any production semantics fix
remain unauthorized; renewal continuity stays unestablished.

**The pins** (current behaviour, as diagnostic assertions):
- **Lifecycle** (`namespace_lifecycle.rs`, managed, explicit instants,
  16 s test term):
  - an expired lease renewed before retirement is extended;
  - race (a): an expired write removes the lease, so the reaper starts
    no retirement;
  - race (b): apply-before-retire. A renewal applied between the
    reaper's enumeration and `begin_retirement` is applied, then
    reclaimed `Abandoned`. The ACK is not exercised there;
  - race (c): re-lease after receipt eviction (capacity, horizon) and
    after a retirement with no namespace.
  - Renewal while retiring or receipt-held: the existing
    `terminal_state_dominates_an_inflight_or_late_lease_renewal`.
- **Store** (`partition_store.rs`, managed, 20 ms term, 60 ms wait):
  - a write between the deadline and the reaper is `QueryLeaseExpired`,
    and the published partitions stay resident and servable after the
    reaper;
  - a write after the reaper is `QueryLeaseMissing`.
- **Server, over the wire:** a `QueryLease` for a retired,
  receipt-held query is answered `QueryLeaseAccepted`. No lease appears,
  the receipt stays, and a later write is `QueryLeaseMissing`. **The
  ACK does not certify application.**
- **Coordinator** (paused clock, 5 ms cadence):
  - a connect, or an ACK, slower than its 100 ms bound fails the query
    at tick + bound;
  - inside both bounds the query stays alive;
  - the connect and operation bounds are separate.
- **Dispatch:** after a lease failure, an in-flight dispatch returns
  `WorkerLost` naming the failed worker, and a later dispatch returns
  `Cancelled`. The lease fails only after the holding worker has read
  `StartTask`, signalled by a `Notify`, never a sleep.

**The managed fixture mode.**
- `toron_fixture_worker --managed-lifecycle` builds the production
  worker's store: `InMemoryPartitionStore::managed(test_default,
  DEFAULT_WORKER_LOST_AFTER)`, with an owned 1 s reaper, as in
  `daemon.rs`.
- Recorded as `lifecycle_mode`, plus `worker_identity` once the
  endpoint is known: id, endpoint, pid, managed, lease term in ms.

**The reaper events.**
- Every pass is recorded as `reaper_pass_start {pass, before}` and
  `reaper_pass_end {pass, after}` on the worker's clock.
- Snapshots carry the aggregate counters and `published_manifests`:
  every manifest this worker published, by exact key, with `resident`
  from a pure `manifest(key)` lookup.
- The before and after snapshots are **separate samples**, not atomic
  around the reaper alone. Concurrent retirement or writes can cause
  transitions between them. So a transition is reported as "resident →
  absent observed during the pass window", and the counter delta as
  "consistent with this query" (exactly 1, and no other query's
  manifest went absent in that window). Neither proves the reaper
  performed it.

**The lease-application markers.** The worker's store is wrapped
(`LeaseApplicationStore`, forwarding every `PartitionStore` method
unchanged).
- Each `issue_or_renew_query_lease` call records `lease_apply_start`
  and `lease_apply_end`, with the query, a per-call ordinal, and the
  active-lease count before and after.
- An end marker is a **call-completion upper bound**, not proof that the
  renewal was applied: applied or ignored is not exposed per query.
- `publish_applied` records each successful publish by key.

**Typed decoding at the production boundary.** `ObservedStream` keeps
whole frames of 256 KiB or less and decodes them passively:
- a `TaskComplete` carrying a `ShuffleProducer` manifest emits
  `task_complete_manifest_{read,written}`;
- `PullPartition` emits `pull_partition_request_*`;
- `PartitionMissing` emits `partition_missing_reply_*`, with the typed
  reason;
- `PartitionData` emits `partition_data_reply_*`.
- Anything else emits nothing.

**Owner disappearance.**
- **Primary:** `owner_disappearance_on_coordinator_path`.
  - The owner is a child process on `execute_query`, running
    `long_join_last`, bound by the first `task_complete_manifest_read`.
  - **Limitation:** on loopback the kill lands before the first 5 s
    renewal tick, so this is an issuance-only owner control.
- **Secondary, reported separately:**
  `scheduler_owner_disappearance_on_loopback`. The scheduler path has
  per-dispatch issuance and no renewals, and is bound by
  `manifest_received`.
- **The harness's own raw log** (`harness_events.jsonl`): owner
  spawned, bound, kill sent, reaped (exit signal and code), wait
  satisfied or unavailable, inspection start and return, absence-window
  end, control end. `owner.json` is a summary that the reader
  recomputes.
- **Kill confirmation:** the owned child, reaped with signal 9 after a
  successful `kill`. An unconfirmed kill is a named "owner-loss
  unavailable" result, with no residency, inspection, publication or
  fanout conclusions.
- **The wait:** the last completed application call for the bound
  query on the producer, plus the term. Every call must be closed, and
  any later call refreshes it. Then the first completed reaper pass
  after it.
  - A truncated producer log withholds the wait.
  - The last `QueryLease` read + term is only a candidate lower bound.
- **Inspection:** one dial to the producer's own endpoint, and exactly
  one `PullPartition` for the exact bound address and attempt, issued
  after the selected pass. Nothing else is written, so it cannot renew.
  - The reply is the decoded `PartitionMissing` reason or
    `PartitionData`. Anything else is an unclassified response; there is
    no substring classification.
  - "Abandonment receipt for the query" means the typed reason is
    `Abandoned`.
- **Orphaned tasks' post-kill publications** for the query are
  reported separately, each with its own pass window.
- **CancelTask / RetireQuery absence** is claimed only within [kill
  sent, absence-window end], across every declared worker. Each log
  must be untruncated and must record an event past the end; the 1.5 s
  coverage tail guarantees a reaper pass there.

**Lease fault controls** (`lease_fault_control_on_loopback`:
coordinator path, `long_join_last`, three managed workers, target = the
first entry; a 5 s post-terminal window, then the 1.5 s tail).
- **Phase delays** (`PhaseDelayTransport`, on `connect_worker` to the
  target only):
  - connect is held before the inner dial;
  - write is gated before bytes reach the observed stream;
  - read is stashed and delivered late (stashed = delivered + discarded
    per ordinal, reconciled);
  - close is held before the inner shutdown.
- **Values:** 150 ms, and 50 ms as a **lower added delay**. 50 ms is
  not guaranteed to stay inside the 100 ms bound: the bound covers the
  whole operation, and timers can overrun.
- **Routes** (a UDP relay on the coordinator's route to the target
  only): none, a 40 ms delay each way, 10 % seeded loss, an 8 s
  blackout, and unreachable. Realized per-direction counts are recorded
  and typed. A lossless, blackout-free route must drop nothing.
- **The blackout trigger:** the target's first positively decoded
  producer manifest (`task_complete_manifest_read`), bound by task,
  exchange and stream. No trigger is "unavailable".
- **Census worker kill** (renamed from `worker_kill`): SIGKILL 3 s
  after the target's stage-0 `StartTask` is written. That usually comes
  after the fast producer's output, while the target stays in the
  census. It is a census-worker-loss control, not a mid-scan loss;
  mid-scan binding is recorded as missed coverage. Its raw signal events
  must name the target and agree with the saved record.

**The timer-overrun limitation.**
- The 100 ms bounds are requested tokio timeouts, not hard bounds.
- A cancelled phase longer than 100 ms is recorded as an overrun, never
  rejected. Configured versus realized holds are diagnostics.
- Development saw 0.7–1.8 ms overruns. The ≈2.4 s overrun in the first
  development `none` smoke is retained and unexplained.
- Durations are censored by the caps. An abandoned dial or operation is
  never a renewal latency.

**Declarations and readers.**
- `schedule.json` (written by `adr628_schedule.py`, then frozen) has
  one independent declaration per attempt:
  - test, control, path, case;
  - the exact worker roster, managed mode and lease term;
  - phase values, relay parameters, blackout and census kill;
  - windows, the reference attempt, and the evidence files that must
    exist.
- `adr628_read.py` validates exactly the declared roster and
  configuration. It refuses on:
  - a missing declared log or an undeclared worker log;
  - an unknown id;
  - a mismatched mode, term, endpoint or configuration;
  - instrument failure;
  - a malformed manifest, partition, query or snapshot (including
    duplicate keys);
  - reaper passes that are duplicated, out of order, overlapping or
    reversed;
  - application calls that are orphaned, duplicated or reversed;
  - a saved summary that disagrees with the raw events.
- Controls get ADR 627's terminal, typed-error and parity checks
  against the same-mode no-fault reference `none-1`.
- 60 reader tests, over real development evidence (golden fixtures,
  one mutation per rule).

**The loopback runner** (`adr628_controls.py`). It runs on the
workstation, drives bigfoot over ssh, and shares the evidence root's
ACTIVE/STOP with the links and Wi-Fi drivers. Per attempt:
1. Refuse if STOP or ACTIVE exists, or if the attempt is not the next
   one in schedule order.
2. Take ACTIVE exclusively, reserve the attempt directory exclusively,
   and write `plan.json` (declaration, exact argv, frozen hashes,
   sequence) **before launch**.
3. Preflight: staged hashes equal the frozen manifest, and no staged
   process runs (the listing itself must succeed).
4. Launch under `timeout -k 10 900` on bigfoot, with a local bound.
   900 s exceeds every internal bound: 120 s query deadline + 360 s
   observation + 6.5 s windows; 30 s bind + 90 s wait. Reaching it is a
   retained `outer_deadline` outcome that STOPs.
5. Copy back, verified against bigfoot's sha256 listing.
6. Postflight: no staged process remains, and no UDP listener is owned
   by any worker pid from the attempt's own logs. A leak is signalled
   only when pid, start time and argv identify it, and it STOPs
   regardless.
7. `record.json` holds rc, timeout, the exact-one-libtest proof and the
   write-failure marker. ACTIVE is released only after verified
   cleanup.
- **Readout:** revalidates order, declaration, argv, hashes, proof,
  cleanup, listing and STOP/ACTIVE absence, then reads every attempt.
- 21 runner tests cover: 0 tests with rc 0, a wrong exact test, an
  altered binary, rc mismatch, outer deadline, a leaked child, a
  pre-launch process, failed ps/ss, a held listener, a missing worker
  log, a missing record, an interrupted driver, unsafe/undeclared/reused
  ids, out of order, altered evidence/stdout/plan.

**Wi-Fi diagnostic (small).**
- ADR 627's driver, host harness and links runner, forked as
  `adr628_wifi.py`, `adr628_host.py` and `adr628_links.py`. Only these
  differ: the evidence root, `--managed-lifecycle` host workers, ADR
  628's schema for the readers, and the frozen set (the `wifi` section
  of `schedule.json`).
- **The set:** one coordinator `long_join_last` reference (bigfoot
  loopback), plus three healthy `long_join_last` coordinator-path
  attempts on the three hosts.
- Links are recorded before and after. The network is unchanged.

### Stage A retention plan (reviewed)

Nothing is deleted or rewritten. A failed or invalid step is kept,
beside an `INVALID.txt` or `NOTE.txt` naming why.

**Before staging**
- A reviewed baseline commit, with the immutable tag
  `adr628-stage-a-baseline` pushed to origin and bigfoot.
- The executables come from that tag's final pinned ladder.

**bigfoot `~/work/toron-evidence/adr628/`** (originals)

| Path | Contents | Kept |
|---|---|---|
| `ladder-N/` | identity (start/end), rc.txt, every step log, release hashes | every ladder: ladder-1 (failed, NOTE.txt), ladder-2 (superseded), ladder-3+ |
| `bin/` | `probe` and `toron_fixture_worker` from the baseline's ladder | frozen; hashes in `manifest.json` |
| `harness/` | `adr628_host.py` | frozen; hash in `manifest.json` |
| `pki/` | ADR 627's identities, copied (same CA) | frozen; hashes in `manifest.json` |
| `controls/<id>/` | each loopback attempt's own output | all 33 |
| `attempts/<id>/` | Wi-Fi attempts (ADR 627 layout) | all 4 |

**Workstation `~/work/.agent-bridge/adr628-evidence/`** (the campaign
root: ACTIVE, STOP, copies verified against bigfoot's listings)

| Path | Contents |
|---|---|
| `schedule.json`, `manifest.json` | frozen copies; their hashes in FREEZE.json |
| `controls/<id>/plan.json` | written before launch: declaration, argv, frozen hashes, sequence |
| `controls/<id>/record.json`, `stdout.txt`, `stderr.txt` | rc, timeout, proof, pre/postflight, cleanup |
| `controls/<id>/evidence/`, `evidence.sha256` | the verified copy and bigfoot's listing |
| `<attempt>/` | Wi-Fi attempts (ADR 627 driver layout) |
| `links-before/`, `links-after/` | link campaigns |
| `STOP` | the first unverified step, if any |
| `readout.json`, `wifi-readout.json` | reader output |
| `SHA256SUMS`, `FREEZE.json` | every file under the root, then the freeze record |

**Frozen dependencies** (copied into `adr628-evidence/readers/` at
freeze; hashes in FREEZE.json):
- `adr628/`: readers, runner, schedule generator, Wi-Fi driver, host
  harness, links runner, ladder script, and their tests and fixtures;
- `adr627/adr627_read.py`;
- `adr626c/adr626c_read.py`;
- `adr625/adr625_read.py`, with the links helpers it holds
  (`LINK_COMMANDS`, `LINK_RUNS`, `links_verdict`).

**Development evidence** (`bigfoot ~/work/toron-evidence/adr628-dev/`,
including `smoke-*`, `ab-*` and `r3-*`)
- Retained as is, never cited as frozen evidence.
- Pre-amendment logs refuse in the reader by construction.
- Four `r3-*` attempts are copied as the reader's test fixtures.
