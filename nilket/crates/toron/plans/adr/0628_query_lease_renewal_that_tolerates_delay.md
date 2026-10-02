# ADR 628 - query-lease renewal that tolerates delay

**Status:** STAGE A COMPLETE: diagnosis delivered, with limited Wi-Fi
renewal coverage (see [Stage A results](#stage-a-results-reviewed)).
Stage B part 1 (B0, lifecycle prerequisites) is DELIVERED as the
reviewed lifecycle/wire implementation (see [B0 results](#b0-results-reviewed)).
B1 (delay tolerance) is scoped (see the B1 amendment): its conditional
reliance contract (B1.A) and test-only timing survey (B1.B) are
authorized for development; its mechanism (B1.C) is not selected, and no
production reliance change, deadline or host campaign is cleared.
Stage B as a whole is not delay tolerance delivered, and the ADR is not
closed as reliability delivered. Successor to ADR 627, which closed with limited
lease coverage.

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
  loopback), plus three `long_join_last` coordinator-path attempts with
  no injected fault on the three hosts.
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

## Stage A results (reviewed)

Accepted on review as the declared diagnostic evidence. Every figure
below was read from the campaign's evidence or the accepted ladder. No
timeout, connection-reuse or worker-lifecycle change follows from these
results.

### Pins (deterministic, accepted ladder-3)

Ladder-3 ran on bigfoot at the laddered source 137a3d21 (tree 0f2f2e64),
dirty 0 at start and end, toolchain 1.95.0, all six steps rc 0:
- toron: 46 binaries, 5621 passed, 0 failed, 17 ignored;
- toron-tpch: 674/0/6;
- toron-client: 22/0/0;
- fmt clean; 0 release warnings.

All 14 ADR 628 pins pass. They fix **current** behaviour; they are not
invariants.
- **Lifecycle (6):**
  - an expired lease renewed before retirement is extended;
  - race (a): an expired write removes the lease, so the reaper starts
    no retirement;
  - race (b): apply-before-retire. A renewal applied between enumeration
    and retirement is applied, then reclaimed `Abandoned`;
  - race (c): re-lease after capacity eviction, after the receipt
    horizon, and after a retirement with no namespace.
- **Store (2):**
  - an expired write leaves the published partitions resident and
    servable after the reaper;
  - a write after the reaper is `QueryLeaseMissing`.
- **Coordinator (4):**
  - a slow connect, or a slow ACK, fails the query at tick + 100 ms;
  - inside both bounds the query stays alive;
  - the connect and operation bounds are separate.
- **Server (2):**
  - a lease failure ends an in-flight dispatch as `WorkerLost` naming
    the failed worker, and a later dispatch as `Cancelled`;
  - a `QueryLease` for a retired, receipt-held query is answered
    `QueryLeaseAccepted` without being applied, and a later write is
    `QueryLeaseMissing`. **ACK ≠ application.**

**Defects carried forward.** The expired-write/reaper path (race a) and
apply-before-retire (race b) are deterministic pin behaviour. The
ordinary-reclamation campaign below neither exercises nor disproves
them. This cleanup defect is a prerequisite scope decision before any
longer-retry mechanism. ACK ≠ application remains relevant to Stage B.

### Campaign (observed incidence)

**Loopback controls.** The coordinator path, `long_join_last`, three
managed workers, target 62801. Readout READ, reasons [].
- **Completed, parity equal to the none-1 reference** (each with 2
  issuances and 6 ACK-observed lease-only streams; operation completion
  unavailable):
  - none ×2;
  - connect, write, read and close at 50 ms, ×2 each;
  - route_none ×2 and route_delay_40 ×2, route isolation observed;
  - route_loss_100-2.
- **Error, `WorkerLost(62801)`:**
  - connect_150 and write_150, ×2 each: 1 issuance, 8 phases, 7
    cancelled;
  - read_150 and close_150, ×2 each: 1 issuance plus 1 ACK-observed
    lease-only stream, 8 phases, 7 cancelled;
  - route_loss_100-1;
  - route_blackout_8s ×2: applied, triggered by the decoded manifest;
  - route_unreachable ×2: permanent;
  - census_worker_kill ×2: signal confirmed.
- **Scope of these findings:**
  - The 50 ms variants are lower added-delay observations, **not** a
    guaranteed safe deadline threshold.
  - The 150 ms controls distinguish failure phases. Phase counts include
    issuance and retirement, so they are not all renewal attempts.
  - Cancelled-phase overruns were recorded, never rejected; per attempt
    the maximum was 1065–2026 µs.
  - The two seeded-loss attempts had different outcomes. Both are
    recorded. The seed fixes the generator, not the realized packet
    schedule, and a relay send is not a delivery.
  - Short and permanent coordinator-route blackouts show the current
    terminal failure, not tolerance or recovery.
  - census_worker_kill is a census loss; its mid-scan coverage was
    missed.
  - In every control, workers read `RetireQuery` within the run's span
    (normal terminal retirement), so no fanout absence is claimed there.

**Owner disappearance.** owner-coordinator-1 and -2 are issuance-only;
owner-scheduler-1 is separate. In all three:
- the kill was confirmed (signal 9);
- the conservative wait (last application-call end + term: a
  call-completion upper bound) was followed by the first completed pass
  after it, pass 37;
- the bound manifest was observed going resident → absent **during** pass
  window 37, after the wait, with an `abandoned_reclaims` delta of 1
  consistent with this query. That the reaper performed it is not
  established;
- inspection, one `PullPartition` for the exact address, returned the
  typed `PartitionMissing` reason `Abandoned`;
- an orphaned post-kill publication (exchange 1, stage 1) appeared on
  another worker at about 30.05–30.07 s, then went resident → absent in
  its own pass window 37;
- no `CancelTask` or `RetireQuery` was read by any declared worker within
  the finite recorded window [kill, window end] (unix ms, one host).

The issuance-only coordinator controls do not establish behaviour after
periodic renewal.

**Wi-Fi.** Readout READ over schedule-r2. Links before and after: 48 runs
each, all rc 0.
- The reference (bigfoot loopback) completed in 30087 ms.
- All three coordinator-path `long_join_last` attempts with no
  injected fault returned `WorkerLost` at 5121 ms, naming 70202, 70203
  and 70203.
  - The required long coverage was **not met**: 0 workers with ≥ 5
    ACK-observed lease-only streams.
  - The observed operations and the current code are consistent with
    the 100 ms caps causing the failure on this setup. The unclassified
    dials do not prove the renewal caller, and "no fault injected" does
    not establish a healthy network.
  - No parity is available for these errors. No failure rate or
    uncensored deadline distribution is claimed.

### Provenance

- **Source:**
  - baseline 5bef11ce (tag `adr628-stage-a-baseline`, tree 3af18f0b);
  - it differs from the laddered 137a3d21 only in this plan;
  - plan commits 21cc07ff and f7165866.
- **Ladders:** ladder-1 failed (a sleep guard in a pin) and ladder-2 was
  superseded; both are retained with notes.
- **Artifacts:** probe 01d84f46…, fixture worker bf5f36b2…. Host
  manifests: bigfoot 961b46ef…, strix b3093516…, optiplex 47336c4a….
- **Freeze:**
  - FREEZE.json b4dbbd48…, schedule.json 9226da40…;
  - FREEZE-r2.json 53df72c8…, schedule-r2.json 3d31d96c…,
    resumption-r2.json 2673a944….
- **Readers:** controls.readout reads schedule.json, whose loopback
  declarations are identical to schedule-r2's. wifi.readout reads
  schedule-r2.json.
- **Readouts:** readout.json 2241d5b9…, wifi-readout.json 090c89eb….
  SHA256SUMS de0cee5b… covers 706 files.
- **Retained outside the r2 acceptance set:**
  - the original `ref-coordinator-long-join-last` (setup_failed: the
    `attempts/` parent was not staged; cleanup_ok=false,
    evidence_complete=false);
  - its clock output `1790934475832021200`, from `date +%s%3N` under
    bigfoot's coreutils, which is not epoch ms; its correlation is
    unavailable;
  - STOP-1.txt, NOTE.txt, and the r2 mapping to
    `ref-coordinator-long-join-last-r2`.
- **Evidence:** the workstation root
  `~/work/.agent-bridge/adr628-evidence`; bigfoot originals under
  `~/work/toron-evidence/adr628`; development runs, never cited, under
  `adr628-dev`.

### Stage B scope amendment, part 1: lifecycle prerequisites (B0) (reviewed)

Stage A's pins fixed current behaviours that a delay-tolerance mechanism
cannot be reasoned about on top of. This amendment proposes to correct
them first, as **B0**: a deterministic lifecycle and wire change. It
selects no delay-tolerance mechanism; B1 stays unselected. B0 changes
expiry enforcement and lease-reply outcomes. It does not change the lease
term, the reaper period, the receipt capacity or horizon, TLS, or ADR
577's idle policy.

#### The defects (code-established, pinned in Stage A; `main` 796546d5)

`ShuffleNamespaceLifecycle` (`namespace_lifecycle.rs`) is shared by the
in-memory, encrypted-spill and hybrid stores. The worker's 1 s reaper
drives it (`server/mod.rs:332` → `reap_expired_queries`).

1. **Expired write, then the reaper (race a).**
   - `authorize_write` on an expired lease *removes* the lease and returns
     `Expired`.
   - The query then has no lease, no retirement and no receipt, so
     `expired_queries` never selects it.
   - Stage A established that the published partitions stay resident and
     servable after that sequence, **until another cleanup event**
     (coordinator retirement, cancellation or process exit), not
     universally until the process exits.
   - Pin: `adr628_expired_write_leaves_published_partitions_resident_after_the_reaper`.
2. **Apply-before-retire (race b).**
   - `expired_queries` removes the expired leases under the lifecycle
     lock. `retire_query` → `begin_retirement` runs afterwards, outside
     it.
   - In that gap `issue_or_renew` sees no lease, no retirement and no
     receipt, and inserts a fresh lease. Writes are authorized again
     until retirement removes it, and the query is then reclaimed
     `Abandoned`.
   - Pin: `adr628_renewal_between_reaper_enumeration_and_retirement_is_applied_then_reclaimed`.
3. **Replies do not say what happened.**
   - A renewal for a retiring or receipt-held query is ignored, yet
     answered `QueryLeaseAccepted`. The coordinator cannot tell applied
     from ignored.
   - Separately, a renewal of an elapsed but not-yet-reaped lease extends
     it.
   - Pins: `adr628_a_renewal_after_retirement_is_acked_without_being_applied`,
     `adr628_renewal_of_an_expired_lease_before_retirement_extends_it`.
4. **Retirement participants are a count, not a set, and are fixed by
   the first caller.**
   - `begin_retirement_with_participants` uses `or_insert`. The hybrid
     store begins with 2; its in-memory and spill children each call
     `begin_retirement` (1, a no-op once a record exists) and then
     `finish_removal`, which decrements.
   - Any path that installs a retirement first, with 1, fixes it at 1.
     The first child to finish then writes the receipt and ends the
     retirement while the other backend still holds data.
   - A repeated retire on an already-removed backend can decrement again.
   - Today the reaper is the only expiry path and calls the hybrid's
     `retire_query`, which begins with 2 first. B0 adds expiry
     observers, so this becomes reachable and must be fixed in B0.

#### B0 semantics

**B0.1 — Logical expiry and physical cleanup are separate, explicit
states.** Per query, the lifecycle holds exactly one of:
- `Leased { deadline }`: writes are authorized while `now < deadline`.
  Expiry is **`now >= deadline`**, and equality is expired.
- `Retiring { cause, pending: BackendSet, receipt_licensed,
  cleanup_queued }`: logically terminal, with physical cleanup pending
  for each backend in `pending`.
- `Receipt { cause, at }`: cleanup finished and a namespace existed.
- Nothing: never seen, or the guard has gone (see B0.3).

The **expire** transition is one critical section under the lifecycle
lock. It applies to a `Leased` entry whose deadline has passed:
1. remove the lease;
2. install `Retiring { cause: Abandoned, pending: the lifecycle's
   declared backend set, receipt_licensed: (namespace seen),
   cleanup_queued: true }`;
3. push the query onto a lifecycle-owned **pending-cleanup queue**.

**Every observer takes it:** the reaper (`expired_queries`),
`authorize_write` (which still returns `Expired` to the writer),
`issue_or_renew` (which replies `Ignored`), and `has_live_lease`. Pending
cleanup is therefore discoverable whichever observer saw the expiry
first.

**An already-terminal cause is preserved, consistently.**
- Expiry never overwrites an existing `Retiring` or `Receipt`.
- `begin_retirement(cause)` on an entry that is already retiring keeps
  the earlier cause and does not reset `pending`.
- The **effective retained cause**, not the cause carried by a later
  request, governs everything downstream:
  - physical cleanup behaviour (for example, cancelling live partitions
    and their readers for a non-success cause);
  - the late-pull and wire missing reasons;
  - the receipt and the reclaim counters.
- So a later `Abandoned` or `Cancelled` retire request on a `Success`
  retirement neither records `Success` while applying
  `Abandoned`/`Cancelled` cleanup, nor the reverse. The retire path reads
  the retained cause from the lifecycle before acting.

**The reaper drains the queue without holding the lifecycle lock while
it takes backend locks.** This preserves today's order, store lock then
lifecycle lock:
1. take the queued ids under the lifecycle lock, and release it;
2. for each id, call the store's retire path (`retire_query` →
   `try_remove_quiescent_*` / `retire_spilled_query`), as today.

The reaper also enumerates `Leased` entries whose deadline has passed
and expires them through the same transition.

**Pending cleanup stays discoverable until every pending backend
completes.**
- The queue is only a fast path. Taking ids off it never erases the only
  reference to the work.
- Every reaper pass also re-enumerates every `Retiring` entry whose
  `pending` set is non-empty, and retries its store cleanup.
- A pass that is interrupted, or that cannot finish (a pinned reader,
  spill deletion debt), leaves the entry `Retiring` with its `pending`
  set intact, and a later pass picks it up again.

**Who wins at the boundary**, defined by state at the lifecycle lock:
- **A valid renewal before expiry wins:** `now < deadline` extends the
  deadline. A renewal at the lock after an expiry observer finds
  `Retiring` and is ignored.
- **Expiry before a renewal wins:** the renewal finds `Retiring` (or
  `Leased` with `now >= deadline`, which it expires first) and is
  ignored.
- **The reply reason comes from the state the renewal observed:**
  - `Ignored { Expired }` when that call performed the expiry;
  - `Ignored { Retiring { cause } }` when it found `Retiring`, including
    an `Abandoned` retirement from an earlier expiry;
  - `Ignored { ReceiptHeld { cause } }` when it found a receipt.
- A renewal racing the reaper's enumeration therefore gets `Expired` or
  `Retiring(Abandoned)` depending on which observer ran first. Both are
  ignored, and writes stay denied either way.

**B0.2 — Participants are a set, and completion is idempotent per
backend.**
- The lifecycle is constructed with its declared backend set: in-memory
  `{Memory}`, spill `{Spill}`, hybrid `{Memory, Spill}`. Every
  retirement, from the reaper, an expiry observer, the coordinator's
  `RetireQuery` or a cancellation, installs `pending` as that full set.
- `finish_removal(query, backend)` removes `backend` from `pending`. It
  is a no-op if the backend was already removed or the query is not
  retiring. The receipt is written only when `pending` becomes empty.
  `begin_retirement_with_participants` and its count go away.
- **Backend completion rules (unchanged in substance):**
  - in-memory completes only when `try_remove_quiescent` removes the
    query: no live reader pinned, and the existing reader guard calls
    `finish_removal(_, Memory)` when the last reader releases;
  - spill completes only when its deletion debt for that query is empty
    (`retry_cleanup_debt` calls `finish_removal(_, Spill)` then).
- **Namespace evidence:** `mark_namespace_present` on a `Retiring` entry
  sets `receipt_licensed`, as today. A writer authorized before expiry
  may commit after it; its namespace is still licensed and its data
  reclaimed.
- **Lock order** is unchanged: store lock, then lifecycle lock. No path
  takes a store lock while holding the lifecycle lock.

**B0.3 — Terminality is scoped to the retained guard; the residual
re-lease cases are documented.**
- While a `Retiring` or `Receipt` entry exists for a query id, no
  renewal, issuance or write revives it.
- **Residual re-lease:** after the guard is gone, a delayed message
  carrying the **same old id** can install a fresh lease. That happens:
  - after receipt eviction, by capacity (4096, possibly well before
    15 minutes) or by the 15-minute horizon;
  - immediately after cleanup of a retirement that never licensed a
    receipt (no namespace on that worker).
  - UUIDv7 avoids accidental collisions between distinct queries; it
    does not stop a delayed message with the same id.
- **What B0 claims:** no resurrection while the guard exists. It does
  **not** claim permanent no-resurrection, and `Applied` does **not**
  identify the namespace incarnation.
- B1 must not bridge a lapse or infer continuity from `Applied` alone.
  Any stronger fencing (incarnation ids, tombstones) is a separate,
  reviewed design.

**B0.4 — The lease exchange contract (a wire change; protocol v3).**
- `QueryLeaseAcceptedPayload { query_id, outcome }` with:
  - `outcome = Applied { term_ms }`, or
  - `outcome = Ignored { reason: Expired | Retiring { cause } |
    ReceiptHeld { cause } }`.
- `ProtocolVersion` goes from 2 to 3. Validation is exact-version: a v2
  peer is rejected at the envelope, before any lifecycle mutation or
  task execution.
- **Both lease paths** (the standalone renewal exchange and issuance on
  a dispatch stream) validate, in order: the envelope version, the
  query identity of the envelope, the payload `query_id`, and the
  outcome. For `Applied`, they also check that `term_ms` equals both the
  requested term and the worker's term. A malformed or unknown outcome,
  a wrong query, or a wrong term is a protocol error (fail closed), and
  is never treated as `Applied`.
- **Renewal on the coordinator:**
  - only `Applied` counts as a renewal;
  - `Ignored` immediately ends the query's reliance on that worker
    through today's `fail_query_lease(worker)`. In-flight dispatches
    then surface `WorkerLost` naming that worker, later ones
    `Cancelled`, as pinned today. The typed reason is recorded in
    telemetry and audit.
- **Issuance:** on `Ignored`, the dispatcher sends **no** `StartTask`.
  - The dispatch fails with an existing non-retryable kind:
    `QueryLeaseExpired` for `Expired`, and `QueryLeaseMissing` for
    `Retiring` or `ReceiptHeld`. `failure_retryable` is false for
    both, so the failure taxonomy is unchanged.
  - When an `ActiveQueryHandle` is present (coordinator path), it is
    also failed with `fail_query_lease(worker)`. Scheduler-only callers
    have none and get only the dispatch outcome.
  - There is **no** automatic retry from ignored to a fresh issuance.
- **What `Applied` certifies:** the operation was applied at the
  worker's linearization point (the lifecycle lock). It does **not**
  promise that no retirement follows, that a write completes, or that
  any computation is cancelled.
- **Reading the ACK** and **completing the whole exchange**
  (`finish_terminal_exchange`) remain distinct, and both stay within
  today's bounds.

#### Rationale for terminal expiry

Terminal expiry is an **ownership and lapse policy**: once the worker's
deadline passes, the worker stops licensing that namespace to anyone.
Deadline extension alone would not invalidate a conservative lower bound
on expiry. The reasons for choosing terminal anyway:
- it makes "lapsed" a fact the worker reports (`Ignored`), instead of a
  race against reaper timing;
- it gives B1 a crisp rule: a lapse ends reliance, and is never bridged.

The plan's existing prerequisites for any boundary still hold: the
renewal was actually applied, on the same valid namespace, under the
stated clock model, with no earlier retirement, restart or
invalidation.

#### Not in B0

- Any delay-tolerance mechanism (B1).
- Timing parameters (the term, the reaper period, the 100 ms bounds).
- What stops remote running tasks after a lease failure (Open
  question 2).
- Incarnation fencing.
- Coordinator recovery, W, and the dispatch-exclusion shortcut.

#### Evidence and acceptance

**Deterministic tests** (explicit `Instant`s and barriers for every
critical interleaving):
- **Deadline equality:** at `now == deadline`, writes are denied, a
  renewal is `Ignored { Expired }`, and the reaper expires the query.
- **Valid renewal before expiry wins;** expiry before renewal wins.
  Reply reasons follow the observed state, including the
  enumeration-race variants (`Expired` versus `Retiring(Abandoned)`).
- **An already-terminal cause is preserved:** for example, a `Success`
  retirement is not turned into `Abandoned` by a later expiry
  observation. A later retire request with another cause applies the
  retained cause's cleanup and reports its missing reasons. Pinned for
  both orders.
- **Interrupted, partial and repeated cleanup:** after an interrupted or
  partial pass, the entry stays `Retiring` with its `pending` set intact
  and a later pass completes it. Repeated passes never complete a
  backend twice. Pinned together with the spill-debt and pinned-reader
  cases.
- **Expired write:** `Expired` to the writer, then `Retiring(Abandoned)`
  queued; the next reaper pass reclaims; a pull returns the typed
  reason; and a receipt is written when a namespace existed (replacing
  the race-(a) pin).
- **No reaper window** (replacing the race-(b) pin).
- **An elapsed lease is not renewable** before the reaper (replacing the
  extend pin).
- **Hybrid:**
  - expiry from each observer installs `pending = {Memory, Spill}`;
  - no receipt and no reopening until **both** backends finish;
  - repeated reaper passes and repeated retires do not complete a
    backend twice;
  - a pinned live reader defers in-memory completion until it releases;
  - spill deletion debt keeps the retirement until the debt clears;
  - a pre-expiry-authorized writer's namespace stays licensed.
- **Spill alone:** the same expiry, debt and receipt cases. Spill was
  "unavailable" in Stage A, and B0 does not ship without it, because
  the change is shared.
- **Wire and transport:**
  - all three `Ignored` reasons over the wire;
  - malformed or unknown outcomes, a wrong query, and a wrong term are
    each rejected;
  - a v2 to v3 mismatch is rejected before any mutation or task
    execution;
  - issuance `Ignored` sends no `StartTask` and fails with the declared
    kind;
  - no ignored-to-reissue retry;
  - coordinator renewal `Ignored` ends reliance on that worker at the
    reply;
  - `Applied` keeps the query alive;
  - over both QUIC and the TCP fixture transport.
- **Property tests (supplemental; a seeded, bounded interleaving of
  renew, write, reap, retire and reader pin/unpin):**
  - every resident namespace has either a discoverable lease entry
    (possibly elapsed, pending observation) or a `Retiring` / cleanup
    record;
  - write authorization never succeeds at or after the **current**
    deadline at its linearization point; renewals legitimately extend
    it;
  - a write authorized before expiry may commit later, and is then
    retired and licensed correctly. Its authorization is not treated as
    revoked.
- **Superseded pins:** removed in the B0 implementation commit, with
  each one mapped to its replacing invariant test in the record. The
  original Stage A artifacts and pins remain in history.

**No-regression controls.**
- A declared rerun of the frozen Stage A loopback schedule on the B0
  build, under fresh evidence paths and a fresh freeze: the same
  declarations, with the readers updated only for the new reply
  outcome.
- Expected: the no-fault and 50 ms variants complete at parity, and the
  owner controls still reclaim with typed `Abandoned`.
- The 50 ms variants are reported as observed outcomes, not as
  guaranteed inside-bound thresholds.
- Any difference is reported, never retried.
- No Wi-Fi campaign is needed for B0.

**Ladder:** pinned, in a dedicated worktree, as in Stage A.

#### After B0: B1 (unselected)

These follow B0, each with its own review, informed by Stage A's Wi-Fi
result (3/3 long queries with no injected fault returned `WorkerLost` at the first
renewal interval under the 100 ms caps):
- a conservative-boundary computation, valid only for `Applied`
  renewals, from exact send markers;
- retries within the boundary;
- an *uncensored* duration diagnostic before choosing any deadline;
- per-worker independent renewal, with a declared whole-query policy;
- connection reuse.

None of them may bridge a lapse or infer incarnation continuity from
`Applied`.

#### Limits carried into B0's record

- Namespace continuity is not established. `Applied` does not identify
  the incarnation.
- Issuance outcomes for scheduler-only callers (a dispatch outcome
  alone) are distinct from coordinator-path behaviour (the dispatch
  outcome plus `fail_query_lease` on the `ActiveQueryHandle`).
- No change is cleared to the lease term, the 100 ms bounds, the
  receipt policy, running-task cancellation, or any B1 mechanism.

### B0 implementation amendment (reviewed)

Recorded before the B0 rerun. The implementation is
`impl: adr 628 b0 lifecycle prerequisites`, tagged `adr628-b0-baseline`.
It is source-equivalent to the laddered tree bc07aafd / a191cbd3
(ladder-b0-3).

**Deviation 1: no separate pending-cleanup queue.**
- `cleanup_work` re-enumerates every `Retiring` entry on each reaper pass.
  That is the guaranteed discoverability mechanism; the plan's queue was
  only a fast path.
- An expiry observed by a write or a renewal, and any interrupted or
  partial pass, is therefore picked up again by the next pass.

**Deviation 2: the unmanaged fixture substrate is a synthetic
exception.**
- `ShuffleNamespaceLifecycle::unmanaged_fixture` has no admission policy
  by design. It keeps its pre-B0 lease bookkeeping and always answers
  `Applied`.
- That `Applied` can represent a no-op and is **not** evidence of managed
  application.
- B0's semantics govern every managed lifecycle. Production construction
  stays guarded by
  `adr543_production_worker_uses_managed_store_and_periodic_abandonment_reaper`.
- It is pinned by `the_unmanaged_fixture_substrate_always_answers_applied`.

**Labels of the evidence.**
- `seeded_serial_sequences_keep_state_discoverable_and_writes_before_the_deadline`
  is a serial randomized state-machine sequence (one thread, no
  barriers). It is supplemental, not a concurrency test.
- The real-clock lock tests
  (`calls_waiting_for_the_lock_across_the_deadline_see_it_expired`,
  `a_publisher_authorized_before_expiry_is_retired_and_licensed_after_it`)
  rely on sleeps and scheduler timing. Nothing in them is a barrier that
  establishes every thread's pre-deadline entry, so they are not
  reproducible barrier-controlled interleavings.
- The code's sampling order is explicit: production time is sampled after
  acquiring the lifecycle mutex. The store test also establishes that
  authorization preceded expiry, because the commit succeeds after it.

**Reply and reader contract for the rerun.**
- An `Ignored` reply ends reliance at the reply: the stream is dropped
  without awaiting a close. `Applied` still requires the whole exchange.
- The rerun declarations require decoded lease outcomes
  (`lease_outcomes = "required"`).
- Each outcome binds to exactly one `QueryLeaseAccepted` envelope with a
  unique key on the same stream and frame. That envelope must answer an
  actual `QueryLease` request for the same query, earlier on the same
  dialled (probe) or accepted (worker) stream.
- `Applied` must carry the declared term, on both sides. On each worker,
  the outcomes it wrote reconcile with its own lease-application
  outcomes.


## B0 results (reviewed)

Accepted on review. Every figure below was read from the B0 evidence or
ladder-b0-3.

**What is delivered.** B0 is delivered as the reviewed lifecycle and wire
prerequisite implementation:
- atomic logical expiry at `now >= deadline`, judged at the lifecycle lock;
- a declared backend set, with completion idempotent per backend;
- every pending retirement re-enumerated on each reaper pass;
- the effective (retained) cause governing cleanup, missing reasons and
  receipts;
- `QueryLeaseAccepted` outcomes on protocol v3, where an `Ignored` reply
  ends reliance at the reply and no `StartTask` follows an ignored
  issuance.

No timeout change, retry mechanism or network-reliability acceptance is
implied.

### Provenance

- **Source:**
  - `impl: adr 628 b0 lifecycle prerequisites` dc35f283 (tree 22879ee9),
    tagged `adr628-b0-baseline`;
  - plan commits f642d402 and 9883ca7f;
  - source-equivalent to the laddered bc07aafd (tree a191cbd3); only
    this plan differs.
- **Ladder-b0-3:**
  - bigfoot `~/work/toron-evidence/adr628/ladder-b0-3`, worktree
    `ket-toron-ladder-628-b0-3`, toolchain 1.95.0;
  - identity bc07aafd / a191cbd3, dirty 0 at start and end; all six
    steps rc 0;
  - toron 5645/0/17, toron-tpch 674/0/6, toron-client 22/0/0;
  - fmt clean; 0 release warnings;
  - fixture worker 63597939…, probe 9c8838ec… (`distributed_process_test-6602c3bd8ddbc607`).
- **Retained ladders:**
  - ladder-b0-1 FAILED: toron-tpch's exhaustive `RuntimeError` match,
    and then the unmanaged-fixture reuse test;
  - ladder-b0-2 SUPERSEDED.
- **Deviations:** no separate cleanup queue (full `Retiring`
  re-enumeration); the unmanaged fixture's `Applied` is synthetic. Both
  are recorded in the B0 implementation amendment.

### Deterministic evidence

These tests cover the refusal paths:
- **Lifecycle:**
  - `the_deadline_itself_is_expired_for_every_observer`;
  - `a_valid_renewal_before_the_deadline_extends_it`;
  - `an_elapsed_lease_is_not_renewable_before_the_reaper`;
  - `an_expired_write_retires_the_query_for_the_reaper`;
  - `no_renewal_window_between_reaper_enumeration_and_retirement`;
  - `an_existing_terminal_cause_is_retained_in_either_order`;
  - `completion_waits_for_every_declared_backend_and_counts_each_once`;
  - `interrupted_cleanup_stays_discoverable_until_complete`;
  - `a_write_authorized_before_expiry_keeps_its_namespace_licensed`;
  - `a_retiring_or_receipted_renewal_reports_its_cause`;
  - `the_unmanaged_fixture_substrate_always_answers_applied`;
  - `calls_waiting_for_the_lock_across_the_deadline_see_it_expired`
    (timing-based, not barrier-controlled);
  - `seeded_serial_sequences_keep_state_discoverable_and_writes_before_the_deadline`
    (serial).
- **Store:**
  - `an_expired_write_is_reclaimed_by_the_next_reaper_pass`;
  - `hybrid_expiry_waits_for_both_backends_through_spill_debt`;
  - `standalone_spill_expiry_is_reclaimed_through_deletion_debt`;
  - `a_pinned_reader_defers_expiry_cleanup_until_it_releases`;
  - `a_later_retire_request_uses_the_retained_cause`;
  - `spill_debt_keeps_the_retained_cause_against_a_later_request`;
  - `a_publisher_authorized_before_expiry_is_retired_and_licensed_after_it`
    (timing-based).
- **Server and wire:**
  - `a_renewal_after_retirement_is_ignored_over_the_wire`;
  - `lease_outcomes_cross_quic_and_tcp`;
  - `a_version_two_lease_is_rejected_before_any_mutation`;
  - `malformed_lease_replies_are_rejected`;
  - `an_ignored_issuance_sends_no_start_task_and_fails_the_dispatch`;
  - `an_ignored_renewal_returns_at_the_reply_despite_a_misbehaving_close`.
- **Coordinator:**
  - `an_ignored_renewal_ends_reliance_at_the_reply`;
  - `an_applied_reply_with_another_term_is_not_a_renewal`;
  - `an_applied_renewal_with_a_stalled_close_fails_at_the_bound`.

The superseded Stage A pins and their replacements are mapped in the B0
review record. The originals remain in history at 5bef11ce.

### No-regression rerun

**Setup.**
- Remote root: bigfoot `~/work/toron-evidence/adr628-b0`.
- Local root: `~/work/.agent-bridge/adr628-b0-evidence`.
- Schedule: `schedule-b0` 458dbd18…, the same 33 loopback declarations
  with `lease_outcomes = "required"`, no Wi-Fi.
- FREEZE.json 2dfb9315…, readout.json a297437f…, SHA256SUMS 35eaffc6…
  (456 files).
- Readers: `adr628_read.py` 66e6882d… (80 tests); runner 38 tests.
- Readout: READ, reasons [].

**Findings.** Compared with Stage A, there is **no observed regression in
outcome classes and completed-result parity on the declared loopback
controls**. This is not a latency or performance equivalence claim, nor a
broader robustness acceptance.
- **Completed at parity:** none ×2; the 50 ms phase variants ×2 each;
  route_none ×2; route_delay_40 ×2; route_loss_100-2. Each had 18
  probe-observed lease replies, each bound to its request and decoded
  `Applied` (term 35001).
- **`WorkerLost(62801)`, as in Stage A:**
  - the 150 ms phase variants ×2 each;
  - route_loss_100-1;
  - route_blackout_8s ×2;
  - route_unreachable ×2;
  - census_worker_kill ×2.
  - The seeded-loss pair split again, one error and one completed.
- **Owner controls** (issuance-only coordinator ×2, scheduler ×1):
  - kill confirmed;
  - resident → absent observed during pass window 37, with a delta
    consistent with the query (pass-window attribution only);
  - inspection `PartitionMissing(Abandoned)`;
  - no `CancelTask`/`RetireQuery` within [kill, window end];
  - 2 owner-log replies bound, `Applied`.
- **Workers:** every worker's written outcomes reconciled with its lease
  applications. A worker with zero replies has unavailable lease-reply
  coverage, not positive renewal coverage.
- **No `Ignored` refusal was exercised by this campaign.** Every observed
  reply was `Applied`. The refusal paths rest on the deterministic, wire
  and coordinator tests named above, not on this readout.

### Limits

- Same-id re-lease after guard eviction, or after a no-namespace
  retirement, remains possible.
- `Applied` does not identify the namespace incarnation.
- The unmanaged fixture's `Applied` is synthetic.
- The lock tests are timing-based; the seeded test is serial.
- The owner controls are issuance-only.
- Stage A evidence is preserved separately
  (`~/work/.agent-bridge/adr628-evidence`, bigfoot
  `~/work/toron-evidence/adr628`).
- B1 is not selected.

### Stage B scope amendment, part 2: delay tolerance (B1) (reviewed)

B1 has three separately reviewed parts:
- **B1.A — the conditional reliance contract:** the expiry bound, the
  reliance states, the use gates and the failure policy.
- **B1.B — an extended-budget timing survey:** a diagnostic with its own
  instrument, schedule and reader review.
- **B1.C — waiting or retrying within the boundary:** a mechanism that
  stays unselected until B1.B's evidence is reviewed. No deadline is
  chosen here.

**Unchanged:** the lease term, the worker-loss window, B0's receipt
policy, and remote task cancellation. No campaign is cleared by this
scope.

**The motivating evidence.** In Stage A's Wi-Fi diagnostic, all three
coordinator-path `long_join_last` attempts with no injected fault failed.
Each returned `WorkerLost` at the first renewal interval under today's
100 ms caps.
- Those durations were censored by the caps.
- "No injected fault" does not establish a healthy network.
- No failure rate is claimed.

#### B1.A.1 — The expiry bound (a conditional contract)

**Assumptions.**
- The worker's monotonic clock advances at most `(1 + ρ_w)` per real
  second.
- The coordinator's lease clock advances at least `(1 − ρ_c)` per real
  second.
- `ρ` is fractional, so 500 ppm = 0.0005, with `0 ≤ ρ < 1`.

**The bound.** From the coordinator-clock send marker `t_send` of a
request whose **whole exchange completed `Applied`**, the worker's lease
cannot expire before the coordinator-clock duration
`D = floor(L · (1 − ρ_c) / (1 + ρ_w))`. So:

`B_w = t_send + D − M`

- `L` is the declared term, positive.
- `M` is a declared margin in coordinator-clock units, positive, with
  `M < D`.
- **`floor` is taken in the coordinator's representable duration unit**,
  named in the implementation (for example whole nanoseconds of
  `Duration`), with the rounding documented there. Seconds are never
  floored dimensionlessly.
- Overflow, an unrepresentable result, or a non-positive `D − M` is
  rejected: no reliance is granted.

**Each marker is per request,** correlated with:
- the query and the worker;
- the requested term and the `Applied` term (which must be equal);
- the request's own response.

**What it bounds, and what it does not.** `B_w` bounds the expiry of
**that** lease only. It does **not** certify:
- that the namespace survives;
- that the incarnation is the same;
- the absence of an earlier retirement, restart or invalidation.

Those are explicit preconditions: no worker restart, no earlier
retirement, and no invalidation since that `Applied`. Any detected
violation fails reliance. Restart guarantees stronger than detection are
incarnation fencing, a separate sub-review.

**ρ is a declared operating assumption, not a proven hard bound.**
- B1 declares a conservative `ρ` (proposed 500 ppm on each host), and the
  claim is conditional on it.
- B1.B records each host's synchronization daemon, its configuration and
  its slew behaviour. `CLOCK_MONOTONIC` is affected by frequency and
  slew adjustments.
- B1.B also records `chronyc tracking` (Frequency, Skew) as context. Those
  are estimates, never proof.
- A host without chrony has unavailable evidence. `M` is never used to
  hide an unbounded rate.

**Suspend.**
- A production proposal for ordinary home machines uses a suspend-aware
  coordinator lease clock (`CLOCK_BOOTTIME`), or a reviewed divergence
  check run before every reliance use and refresh.
- Detection can fail only on resume, before further use. It cannot
  promise that a timer ran during suspension.
- The clock's read and sampling uncertainty is specified.
- A clock read that fails or is missing fails reliance.
- "No coordinator suspend" is acceptable only as an explicit,
  limited-campaign assumption, not as general support.

#### B1.A.2 — Reliance states per (query, worker)

`Unissued → Issuing → Reliable(B_w) → Lapsed | Terminal`

- **First issuance.** There is no previous `B_w`. No dependent task or
  partition use is allowed until a validated `Applied` exchange completes
  **and** `now_c < B_w` computed from **that** send.
- **Renewal (preserving B0).** `Applied` refreshes `B_w` only after the
  whole exchange succeeds, and only if processing finds `now_c` before
  the **old** boundary. An observed ACK alone is insufficient.
- **Lapse is irreversible.** A refresh processed at or after the old
  boundary does not restore reliance, even if a starved timer never
  recorded the lapse. Equality counts as lapsed.
- **A refresh must also be usable now.** When a refresh is processed, the
  bound it would install must be in the future (`now_c` before it), in
  addition to the old-boundary and state checks.
- **Out-of-order replies.** An out-of-order `Applied` must never replace
  a valid bound with an already-elapsed one while the state is labelled
  `Reliable`. The explicit preserve / reject / monotone-bound rule is
  defined at the B1.C implementation review.
- Nothing ever restores `Lapsed` or `Terminal`.
- **`Ignored`** fails at the decoded reply, as in B0.
- **No silent reissue after a lapse.**
- **To pin:**
  - old-boundary equality;
  - a late ACK;
  - a timely ACK with a late or failed close;
  - out-of-order responses;
  - a request/send-marker mismatch;
  - a concurrent lapse versus refresh;
  - first issuance before any `Applied`.

#### B1.A.3 — Use gates and failure policy

**The claim.** No reliance-dependent action is authorized after the
boundary is observed at its use gate. Publishing the query's error can
be late under scheduling stalls, and remote and in-flight work is
**not** stopped by this claim.

**Gates** (an inventory to complete in implementation review; each gate
defines its authorization or acceptance linearization point):
- dispatching a task to `w`;
- inserting or publishing an accepted manifest into routing state;
- routing a consumer to `w`'s partitions;
- replay and re-dispatch paths;
- accepting a completion whose output lives on `w`;
- forwarding each buffered final-result batch;
- terminal success of the result stream;
- any work resumed after an `await`. A check made before an `await`
  does not authorize state mutation or output after it.

**Required workers** are bound explicitly. The proposed conservative
policy: every census worker of the query.

**Streamed results.** `execute_query_stream` returns a
`DistributedResultStream`, and final batches are forwarded while the
query runs, so rows can escape before a lapse.
- After a lapse, every later batch is suppressed and the stream ends
  with the lapse error. That is partial-result-then-error semantics,
  stated in the API documentation.
- Collected `execute_query` returns the error and no result.

**Lapse reason.** A typed internal cause names `w` (with the boundary
and whether `Ignored` or expiry), mapped to an existing outward failure
kind (`WorkerLost { w }`) if that is sufficient. A new serialized
`TaskFailureKind` would need its own wire, version and architecture
review. It is never an untyped string.

**Enforcement** combines a best-effort timer with gate enforcement.
Retirement fanout is unchanged. There is no exclude-and-recover.

#### B1.B — Extended-budget timing survey (its own instrument review)

A test-only survey with no production timeout change. Completed samples
are uncensored within its budget; samples that hit the deadline are
censored and reported separately.

- **Budgets.** Each phase (connect, write, ACK read, close) has a budget,
  and the **whole exchange** has a frozen bound below the synthetic
  lease's remaining validity. Four phase budgets plus the cadence must
  not exceed `L`.
- **The synthetic-query lifecycle.**
  - Fresh synthetic query ids are issued.
  - Each is renewed only while its last `Applied` is valid.
  - Each is retired at a declared age.
  - Nothing is renewed forever on an expired or forgotten id, and a
    fresh issuance is never classified as a periodic renewal.
  - The classification never bridges a lapse.
- **Every outcome is recorded:** `Applied`, `Ignored(reason)`, transport
  errors, budget expiry, and the per-phase instants with the exact send
  marker.
- **Load condition.**
  - Production `long_join_last` can fail at about 5 s.
  - The survey records each load query's actual task-activity windows
    and outcomes, and tags each sample with the load state it overlapped.
  - A repeated or independently maintained load is declared and bounded.
  - It never claims "under a running query" for samples after that
    query ended.
- **Idle** means no concurrent campaign load or link tests. It does not
  mean all home traffic is absent.
- **Concurrency and cadence.** The number of overlapping probes is
  declared (none, or a stated concurrency). There is no catch-up burst
  after an overrun: a skipped tick is recorded.
- **Cleanup.** Synthetic ids are retired, and each host's reaper
  evidence is retained.
- **Clock context per host:** the daemon, its configuration, and `chronyc
  tracking` / `sources` before and after.
- **Output:** per phase, pair and condition, distributions with censored
  counts and errors separate. It recommends nothing.
- **Counts, budgets and conditions** are frozen at B1.B's instrument
  review. The example values here are not launch authority.

#### B1.C — Waiting or retrying within the boundary (unselected)

These candidates are evaluated against B1.B's evidence in a separate
review:
- per-worker independent renewal, instead of the sequential census;
- per-attempt connect and operation budgets as an operational choice
  from B1.B's distributions, including censored counts and errors, never
  from success-only samples. A quantile plus a margin is an operational
  budget, not a safety proof;
- retries only while an attempt can complete before the current `B_w`;
- an earlier renewal cadence, with the term unchanged;
- connection reuse, as an optional sub-review.

#### Acceptance outline (declared with B1.C; conditional on the selected mechanism)

**Deterministic tests:**
- the B1.A.2 state pins;
- every use gate refuses after the boundary even with timers starved;
- streamed output stops after a lapse;
- first issuance gates dependent use;
- `ρ`/`M`/floor arithmetic;
- the suspend handling as declared;
- per-worker independence, only if that mechanism is selected.

**Campaign:**
- Wi-Fi long queries with no injected fault complete at parity over a
  frozen count. Failures are reported, never retried away.
- **Blackout survival** is claimed only when a valid `Applied` exchange
  completes before the old boundary. Remaining slack, scheduling and
  reconnection consume time, so the blackout duration alone proves
  nothing.
- Asymmetric coordinator and consumer routes.
- An unknown application after an ACK loss.
- Out-of-order and stale responses.
- Timer starvation with streamed output.
- **Permanent unreachability** reports three things separately:
  - when gates stopped reliance;
  - when the error was delivered;
  - when the workers reclaimed.
- ADR 627's coordinator mid-task cases are rerun. That claims nothing
  about stopping running tasks.

**Never conflated:**
- bounded cessation of coordinator reliance at the gates;
- remote reclamation (B0);
- running-task cessation, which is reported as observed only.

#### Not in B1

- Stopping remote running tasks after a lapse.
- Incarnation fencing.
- Coordinator recovery, W, and the dispatch-exclusion shortcut.
- Changes to the term, the loss window, receipts or the reaper.

### B1.B survey instrument amendment (reviewed)

Recorded before the survey runs. This is test-only: the source change is
confined to `tests/distributed_process_test.rs`
(`diag: adr 628 b1b timing survey instrument`, tagged
`adr628-b1b-baseline`). It is source-equivalent to the laddered
242a94bf / 8827e5fc (ladder-b1b-3).

**What it measures.** Each lease exchange is timed as five phases, each
with an explicit start and exactly one terminal mark (end, budget
expiry, error, or the whole-bound cut on the active phase):
- **connect**;
- **write:** its start is the exact send marker;
- **ACK read:** length-checked; a frame over 64 KiB is a protocol error;
- **validate:** decode, `validate_runtime(CURRENT, 64 KiB)`, kind,
  identity, query, outcome and term;
- **close.**

The phase budgets are 5000 ms each, and the whole exchange is bounded at
20000 ms. Every outcome is data, and censored samples and failures are
counted separately. The survey reports distributions and recommends no
deadline.

**Synthetic-id lifecycle** (one per-worker state machine, concurrency 3):
- **Ticks:** fixed schedule (5000 ms) with `MissedTickBehavior::Skip`. Each
  tick's scheduled and actual instants are recorded, and missed intervals
  are recorded, including at the end. There are no catch-up bursts.
- **One action per tick:** issue, renew or retire.
- **Renewal** happens while age < 14000 ms (the decision policy, not an
  exact remote lifetime), renewal attempts < 4, and `decision + whole <
  last accepted send + term`.
- **The validity boundary is nominal:** `send + term` on the coordinator's
  steady clock, with ρ = 0, no suspend and no restart. It is diagnostic
  only, NOT B1.A's reliance boundary.
- **Refresh.** An `Applied` refreshes only when the whole exchange
  completed before the old nominal boundary and before its new boundary.
  Otherwise it is kept as data and the id is retired as "refresh
  rejected".
- **Retirement.** Every attempted id is retired exactly once, with a
  declared reason: age, attempts, invalid, ignored, unknown application,
  refresh rejected, or survey end. The result is recorded verbatim and is
  never a reclamation claim.
- **Counts.** Attempts and refreshes are counted separately (`survey_ledger`).

**Load condition.** `long_join_last` runs back to back on the coordinator
path and logs into the survey's own log, so there is one clock. Each
window binds exactly one query and its raw terminal. Samples are tagged by
observed task activity, invocation without activity, or outside any
invocation (remote activity not excluded). Failed load queries are kept.

**Readers** (`adr628b1_read.py`, 54 tests). The reader:
- validates the whole log before grouping;
- requires full declared-duration coverage, refusing premature finishes
  and skips recorded early;
- replays the state machine exactly: actions, reasons, refresh decisions,
  ledger presence and placement, final retirements;
- requires strict JSON integer types;
- binds decoded outcomes in order, one-to-one, to each worker's own lease
  applications, with Ignored cause and Applied term. Unmatched worker
  applications are counted, and a missing or truncated worker log makes
  this cross-check unavailable.

**Driver.**
- Survey workers get a lifetime covering setup, gates, clock captures,
  the bounded probe (duration + 600 s) and cleanup.
- Per-host clock context (sync daemon, configuration hash, chrony
  estimates) is captured before and after. It is context only, never a ρ
  bound.

**Frozen parameters:** idle and load ×1 each; 1800 s; links before and
after; the B1 evidence roots.

**Ladders:**
- ladder-b1b-1 SUPERSEDED;
- ladder-b1b-2 FAILED on the same tree: three `sql_listener` timing tests
  and one tpch parquet read. Retained with NOTE.txt; load sensitivity is
  a hypothesis, not an established cause;
- ladder-b1b-3 accepted. Release hashes differ between worktrees, and
  embedded paths are a hypothesis.

B1.C remains unselected.

## B1.B results (reviewed)

Accepted on review as the B1.B timing diagnosis. It is complete under an
explicitly post-run amended accounting contract (below). Every figure was
read from the B1 evidence. The source is unchanged from
`adr628-b1b-baseline` (37a3044d); this record changes only this plan.

### Frozen readout: REFUSED

The frozen readout (`readout.out`, 22828414…) is **REFUSED**, with one
reason: `survey-load-1: 70202: lease outcomes`.

B0's worker reconciliation requires the multiset of lease applications to
equal the multiset of written outcomes. Under load, the strix worker
recorded 667 applications and 665 written outcomes.

The two unwritten applications are load-query renewals, not survey samples:
- call 191, stream 327, query `01a0fd87-9700-…`;
- call 208, stream 356, query `01a0fd88-4ab2-…`.

In each case:
- the worker read the `QueryLease` and applied it (`Applied`);
- the same stream then ended with `write_error` "connection lost", with
  zero recorded outbound frames and bytes;
- a `RetireQuery` for the same query followed on a new stream.

Peer receipt and the cause of the connection loss are not established.

The frozen result stands. Its reader, FREEZE and evidence are unchanged.

### Post-run amended accounting contract (`b1b-reconcile-2`)

This was authorized after the run, as an opt-in B1.B reader. B0's function
and its records are unchanged.

**Written outcomes** still consume applications, with B0's outcome
normalization. Written without an application still refuses.

**A failed reply** is the only exception. An application without a written
outcome is accepted only if all of the following hold:
- its outcome is exactly `Applied`;
- it is bound to a served stream with exactly one `stream_accepted`, one
  `QueryLease` envelope read, one payload read for that frame, and one
  `stream_end`;
- the order is accepted → envelope → payload → application start →
  application end → stream end, by log position AND same-worker timestamps;
- it is the only application inside payload-read → stream-end, and that
  interval is the only one enclosing it;
- the stream ended `write_error`, with `frames_written` and `bytes_written`
  exact integer 0, no partial written frame, and nothing written on it.

**Classification is all or nothing:**
- every application is consumed once;
- any candidate failing these checks refuses;
- a truncated log refuses.

**Classification:** exactly the two cases above, as "application Applied;
no complete reply recorded; stream ended write_error (zero recorded
outbound bytes/frames)". They are not deliveries, receipts, refreshes or
completed exchanges. The other five worker logs reconcile exactly.

**Tests:** 31, all passing.
- Positives:
  - the real strix sequences, as a verbatim excerpt that is regenerable
    from the evidence;
  - all six retained worker logs;
  - a matched written `Ignored(Expired)`.
- Negatives, each also checked through the frozen parser and the amended
  driver substitution:
  - counters: frames 1, bytes > 0, partial frame, boolean zero;
  - stream end: `read_error`;
  - written evidence: a written frame or outcome;
  - request binding: wrong query, missing accepted, missing request or
    payload;
  - order: accepted or payload out of order;
  - duplicates: duplicate accepted or payload;
  - application pairs: missing or reversed;
  - intervals: application outside the interval, two applications in one
    interval, two intervals for one application;
  - truncation;
  - written without application;
  - an `Ignored(Expired)` application.

**Amended readout:** READ (36d1aa59…). The frozen REFUSED outcome is
recorded at the top.

### Survey exchanges

- **Counts:** 810 exchanges per condition, 270 per worker.
- **Outcomes:** all decoded `Applied` and refresh-accepted. There were no
  failures, no censored samples and no refresh rejections.
- **Per worker and condition:** 359 ticks, 0 skipped, 90 ids. 89 ids were
  retired by age and 1 at survey end; all 90 retirements were "retire
  acknowledged".
- **Attempt limit:** age came before the attempt limit, so each id had one
  issuance and two renewals. The limit of 4 was not exercised.
- **Largest whole exchange:** idle 1008207 µs (an optiplex issuance); load
  773868 µs (an optiplex issuance).

Per-phase p50/p99/max (µs), for the remote workers:

| Condition | Worker | Kind | connect | ack | whole |
| --- | --- | --- | --- | --- | --- |
| idle | strix | issuance | 158058/321412/321412 | 142584/315905/315905 | 302325/538738/538738 |
| idle | strix | renewal | 154627/316724/339174 | 116107/381159/510010 | 312062/626002/750303 |
| idle | optiplex | issuance | 156731/528052/528052 | 132776/480136/480136 | 293300/1008207/1008207 |
| idle | optiplex | renewal | 163744/369836/435301 | 113415/410996/510637 | 288852/711639/751779 |
| load (task activity) | strix | renewal | 126038/353886/366351 | 102544/281915/305021 | 238174/556541/576923 |
| load (task activity) | optiplex | renewal | 132163/329984/366813 | 100674/310182/464283 | 250575/548331/559179 |

- **Other phases:** write, validate and close stayed at or below 237 µs.
- **Same-host worker** (bigfoot): whole p50 about 1.4–1.5 ms, max 2394 µs.
- **Above 100 ms:** the remote workers' connect p50 and ack p50 were each
  above 100 ms in both conditions.
- **What these are:** extended-budget exchanges on the survey's own
  connections. They are not the load query's connections and not
  production per-phase caps. They select no deadline, reuse policy or retry
  mechanism, and no causal explanation is drawn from them.

### Load windows

**Scope.** These are the declared back-to-back query-invocation windows.
They are not completed long-query load and not a general reliability rate.

- **All 330 windows returned an error** with `WorkerLost`: 176 named strix
  (70202) and 154 named optiplex (70203).
- **Terminal minus start** ranged from 5102006 µs to 10301264 µs (p50
  5103486 µs).
- **Completed-load coverage was therefore not obtained.** It remains a
  finding and a limitation.
- **Worker context** (separate clocks):
  - strix served 330 load task streams, with 0 open at log end;
  - optiplex served 330, with **5 open at log end** (possible orphans, not
    proven running tasks).

### Context

- **Clock:** chrony tracking was unavailable on bigfoot and strix. On
  optiplex it read 0.24–0.76 ms slow and 11.58 ppm slow. This is context,
  not a ρ bound. The survey boundary is nominal (ρ = 0, no suspend, no
  restart).
- **Links:** before and after, 48 measurements each, all rc 0.
- **Run:** both attempts report `ran`, cleanup ok and evidence complete.

### Provenance

- **Source:** `adr628-b1b-baseline` 37a3044d (tree 082baa30); plans
  ed1742f0 and a4df3888. Source-equivalent to the laddered 242a94bf /
  8827e5fc.
- **Ladder-b1b-3:** toron 5649/0/19, toron-tpch 674/0/6, toron-client
  22/0/0; fixture worker 416ad591…, probe 68e68b99….
  - ladder-b1b-2 FAILED and is retained;
  - ladder-b1b-1 SUPERSEDED.
- **Evidence root:** `~/work/.agent-bridge/adr628-b1-evidence`.
  - FREEZE 384a1c6e…;
  - SHA256SUMS bc46daf1… (281 files).
  - The links runner's root fix and the regenerated pre-notice FREEZE are
    recorded in `PROVENANCE-freeze.md`.
  - The remote roots are `~/work/toron-evidence/adr628-b1` on all three
    hosts.
- **Amendment bundles:**
  - `amended-1` (`b1b-reconcile-1`; SHA256SUMS b56c9b89…, 87 files) is
    SUPERSEDED. It was held for causal order and the `Applied`-only
    restriction.
  - `amended-2` (`b1b-reconcile-2`; SHA256SUMS 5421a4e7…, 87 files) is the
    reviewed contract. It holds the reader, tests, fixture extractor,
    generating command and readout 36d1aa59….
  - Its readout differs from amended-1's only in the reader version,
    the added payload-read instant and the provenance hashes of the
    three changed reader files. My first statement of the differences
    omitted those hashes; it was corrected in review.
- **Diagnostic, not acceptance evidence:**
  `DIAGNOSTIC-not-frozen-load-readout.json`.
- **Test-suite gap:** the frozen reader copies lack the schedule files
  that two older suites need. Those suites pass from the working
  directory, and the frozen copies are not retrofitted.

### Limits

- No deadline, reuse policy, retry mechanism or causal explanation is
  selected.
- No production reliance change is made.
- B1.C remains unselected, and ADR 628's delay tolerance is not delivered.
