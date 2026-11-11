# ADR 0713: a table write waits, bounded, for a lease that is temporarily unavailable

Status: proposed, 2026-11-11 (Claude, Codex). Source cleared (Codex 01a1c83a). Implementation lands with this plan. Direction: Codex 01a1c7fa. Design cleared with two implementation constraints: Codex 01a1c806 (`reviews/lease-gate/CODEX-DESIGN-CLEAR.txt`). Source review round 1, three corrections (Codex 01a1c82b, `reviews/lease-gate/impl/CODEX-REVIEW-R1.txt`), folded in below. Implementation lands with this plan. Review record: `reviews/lease-gate/` (`CENSUS.md`, `DESIGN-0713.md`, `CODEX-DESIGN-R1.txt`, `ADDENDUM-1.md`). This ADR is the as-built consolidation of those; where they differ, this text and the later ruling govern.

## Why
- **A demonstrated availability failure.** In one single-node run, two table writes ended WRITE_ATTEMPTS_EXHAUSTED / KNOWN_NOT_COMMITTED. The Today lease gate answered `Missing` on all three attempts of each, within about 100 ms.
- **The gate could not wait.** `Missing` had three producers and one answer: no cached lease matching holder, epoch and range; a matching lease expired against the client's HLC; no current leader. It returned at once. The client then spent its three attempts, 25 ms apart, inside one renewal interval (1 s at the 3 s TTL). On the last attempt the server records the refusal durably under the write's identity, so that identity can never commit afterwards.
- **What is not known.** Which producer fired in the incident, and what delayed renewal. Late renewal is a hypothesis, not a finding. The old behaviour is not called a correctness defect: bounded retries are allowed to refuse. This ADR adds a stated availability contract.

## Decision
**In Today mode, a table write or table delete whose lease gate answers `Missing` waits at the gate for a lease to be cached or for the leader to change, under one absolute deadline of at most `LEASE_GATE_WAIT` = 2 s. A gate that answers `Valid` again admits only after the write's authority has been established again from the top. At the deadline the existing LEASE_EXPIRED refusal stands.**

1. **Where.** The gate site of `write_table_bifurcated` (`network/data_server.rs`), which table delete shares. The gate, the wait and the readmission live in `network/data_server/write_replication.rs` (`admit_at_table_lease_gate`, `wait_for_lease_gate`, `readmit_after_lease_gate_wait`). With a lease and a leader in place the path is one pass over the children, as before.
2. **The wait.**
   - **One deadline.** Now plus the cap, shortened to the write's own `WriteClocks` expiry where that is sooner. It is computed once and never renewed on a wake. The caller's clocks are read, never advanced and never rebuilt.
   - **It covers evaluation too.** Every gate evaluation, its leader read included, runs under the same deadline as every sleep. An evaluation still pending at the deadline is the refusal, and a `Valid` that arrives at the deadline does not win.
   - **Wake sources.** A new notification raised by `cache_granted_lease`, the only production writer of a grant; and a change of leader on the Raft metrics watch.
   - **No lost wake.** Both sources are marked seen before each evaluation, so a grant or a change of leader that lands after the gate was read and before the sleep begins ends that sleep at once.
   - **Only the leader's identity counts.** The metrics watch carries every Raft metric. After an evaluation the newest value is consumed and the leader compared: the gate is read again only if the leader itself is now another, and only such a change ends a sleep. Unrelated metrics cause no evaluation. Stated limit: a leader that leaves and returns to the same identity wholly inside one evaluation is not seen as a change; that wait ends at its deadline, in the refusal.
   - **A closed leader watch refuses** with LEASE_EXPIRED and is never armed again.
   - **`InvalidSignature` is never waited on.** It is refused at once, before a wait or during one.
   - **The wait starts no renewal.** Renewal, the TTL and the lease manager are unchanged.
3. **Readmission after the wait, in this order.** Nothing here waits for a lease a second time.
   1. **Recorded-outcome replay first.** An outcome recorded for the same write while it waited is returned as recorded. It is never replaced by a refusal computed afterwards.
   2. **Epoch.** The admission epoch is read again. A request epoch behind it is EPOCH_STALE.
   3. **Routing and primacy.** The replica groups are computed again from a fresh migrations snapshot and the router. The node must be primary for all of them, else NOT_LEADER.
   4. **The orphan-witness barrier and the pending-finalization replay,** as before the wait.
   5. **Every child's gate once more,** under the wait's own deadline. This pass, not the wake, is what admits.
   6. **The caller's clocks,** last, before any durable work.
4. **One exit for every write that waited and is not admitted** (`lease_gate_exit_unadmitted`): the cap, the caller's clocks, an invalid lease, a closed watch, and every refusal of readmission. What is answered is decided there, in this order.
   1. **Replay.** An outcome recorded for the write, or a pending finalization of it, is returned as it stands. Nothing is recorded over it: no clock expiry, and no last-attempt exhaustion, because the answer is the record.
   2. **The caller's clocks.** If they have expired theirs is the outcome, at a tie with the cap too. This holds for the epoch and primacy refusals of readmission as well.
   3. **The refusal** the exit was reached with.
   - **In readmission's final pass the deadline is judged before the result, whatever the result.** A `Missing` or `InvalidSignature` that is only ready at or after the deadline is the deadline's answer, as a `Valid` is.
   - The replay and the record are two steps, not one transaction. Serializing outcomes globally is not part of this ADR.
5. **Outcomes.**
   - **The cap ends the wait:** LEASE_EXPIRED, then the unchanged last-attempt rule (WRITE_ATTEMPTS_EXHAUSTED, KNOWN_NOT_COMMITTED, recorded). The gate precedes all durable work, so that knowledge is exact for this attempt.
   - **The write's clocks end the wait:** the existing pre-durable outcome, WRITE_DEADLINE_EXCEEDED / DEADLINE or WRITE_STALLED / NO_PROGRESS with KNOWN_NOT_COMMITTED, recorded. Never LEASE_EXPIRED and never left to attempt exhaustion. At a tie the clocks' outcome is the one given.
   - **Nothing is admitted without a `Valid` gate answer.** The predicate is unchanged: holder, epoch, range, `client_hlc < lease_expiry`, issuer = current leader, signature. The wait admits nothing that a later retry of the same request would not be admitted by today.
   - **`commit_hlc` is not restamped.** It is already bound into row identity and payload. An admitted write can carry a stamp older by up to the cap. Limited claim: existing asynchronous barriers already separate stamping from persistence; no new recency guarantee is accepted, and the client-HLC lease predicate is not validated as server-time authority.
6. **Identity.** The waiting attempt is the same request: the same parent write id, digest and `attempts_remaining`. Nothing is re-sent and no identity is minted. The client's loop and its own handling of uncertainty are untouched.
7. **Observation.** `Missing` now names its producer internally: `no_lease`, `expired`, `no_leader`. Selection is unchanged: any covering entry that has not expired is taken wherever it sits, and only when there is none is the miss named, `expired` if some entry covers the holder, epoch and range. One counter family, `wendelon_data_ring_lease_gate_waits_total{reason,outcome}`, is incremented once per completed wait, whatever the number of wakes: the reason seen at entry, by `admitted`, `refused_at_cap`, `refused_invalid_lease`, `clocks_expired`, `refused_on_recheck`, `replayed`, `watch_closed` (a fixed 21 series). The wire status is unchanged.
8. **Held while waiting.** The waiter holds no lease-cache guard. It keeps what the write already holds at the gate: the tenant request admission, one mutation work permit, the mutation drain guard, and for a write the secondary-index admission read guard and, for a content-hash table, that table's admission mutex. **No cycle was found in the traced paths:** Today renewal on a leader and the lease apply arm take none of these (`ADDENDUM-1.md` R3, with its stated limits). Stated cost: during a lapse up to `available_parallelism` waiters occupy every mutation slot for up to the cap, one content-hash table's writes queue behind its waiter, and a queued index declaration cut waits for the waiters and holds new table writes behind it.
9. **The cap is a design value, not a guarantee.** A live lease manager can be stalled inside a pass or unscheduled, and renewal itself has no deadline. From the diagnosis timeline the incident's lapse lasted more than 1.72 s and at most 3.28 s after the first refusal, so one 2 s wait is not shown to cover it.
10. **Out of scope.** KV writes and linearizable reads keep their immediate answer. ParentDecisions is unaffected: none of the gate's callers is reached in that mode. The TTL, the client's retry count, and renewal are unchanged.

## Acceptance (finite)
Runs are on bigfoot against base 91cdddfb0 with the candidate patch applied to a fresh tree. Tests are in `network::data_server::tests::lease_wait`.
1. **The RED.** `a_table_write_waits_out_an_expired_lease_and_commits_once`: through the frame handler, on its last attempt, a write meets an expired lease; a real renewal arrives inside the bound; that attempt answers OK (run `adr0713-r7`, 20 of 20, on the corrected patch), the row is written once, and a replay of the same identity answers from the record and appends nothing. On main's unchanged production code it fails with WRITE_ATTEMPTS_EXHAUSTED (run `adr0713-r1`).
2. **The controls.**
   - Each reason, deliberately (`expired`, `no_lease`, `no_leader`): recovery in the same attempt; and with no authority arriving, refusal at the bound with KNOWN_NOT_COMMITTED recorded, no row, and one count under that reason.
   - The gate names its miss, and a valid entry wins over an expired one ahead of it.
   - A caller deadline inside the cap: WRITE_DEADLINE_EXCEEDED with KNOWN_NOT_COMMITTED, recorded, nothing admitted.
   - Primacy lost during the wait, through the real routing input (`RingRouter::install`) and a real renewal: NOT_LEADER, nothing admitted. The admission epoch advanced during the wait: EPOCH_STALE, nothing admitted.
   - An outcome recorded during the wait is returned, with primacy moved away as well so that validating authority first would have answered differently.
   - The same on the exits that admit nothing: an INDETERMINATE outcome recorded while the waiter is installed, no renewal given, and the wait ended by the cap (on the last attempt), by the caller's deadline, and by an invalid lease. The answer and the stored outcome are both still the recorded one.
   - Readmission with a deadline already passed: with the caller's clocks expired, a stale epoch, lost primacy, and a final pass answering `Missing` or `InvalidSignature` at once all give the clocks' outcome, recorded; with only the cap passed, a final pass that is not `Valid` is the cap's refusal.
   - Two children under two leases: a grant for one child and a grant for another epoch admit nothing, and the final pass reads every child.
   - The leader branch, through the leader override only: another leader than the issuer is refused at once.
   - One DELETE: it recovers in the same attempt with the result and three counts of a delete that never waited.
   - The ordinary path: one pass, no waiter, no count.
   - The wait alone, in virtual time: a wake between the evaluation and the sleep ends the sleep at once (by grant and by leader); wakes that do not help leave the original deadline in place, exactly; a closed leader watch refuses and never spins; the deadline covers an evaluation and a result at it does not win; metrics that move without the leader cause no evaluation, during an evaluation or a sleep, and a grant and a real change of leader each still wake once; a leader that changes during an evaluation is evaluated again at once; the deadline is the cap shortened by the write's own clocks.
3. **The revert.** With the wait reverted behaviourally (`Missing` refused at once), the RED and the recovery controls fail (run `adr0713-r8`: 10 of 20 fail; the node-free wait tests, the ordinary path, the gate-naming test and the direct readmission control pass).
4. **The focused set** passes (run `adr0713-r9`: 667 of 667, in parallel; the data-server, lease and ParentDecisions cluster tests, `three_node_workflow`, `architecture_test`, `lease_signing`, `control_ring_rpc`, `client_retry_topology`).
5. **One finite load check:** the new tests with `--stress-count 30`, once (run `adr0713-r6`: 30 of 30 iterations of the then 16 tests passed, on the patch before source review round 1; not repeated after it). Every failure is reported; it is not repeated.
6. Source review, then one checkpoint at the tip. No memory campaign is a gate.

## Not established
- **That the cap is enough.** It is not a guarantee that a renewal arrives. A lapse longer than the cap, a stuck renewal pass, a stopped lease manager, or a client HLC that stays ahead of every expiry all still refuse.
- **The incident.** Which `Missing` producer fired, what delayed renewal, and whether this change would have altered that run.
- **§8.** Unaffected: it stays FAILED, and no rerun follows from this ADR. No claim is made about the allocator.
- **Lock freedom in general.** "No cycle found in the traced paths" is the whole claim. Not traced: openraft's internals and the log store's append and fsync under `client_write`; apply of unrelated entries ahead of a lease in the serial log; a follower's lease request admission at the leader.
- **Whole-write and whole-operation time.** The cap bounds the lease wait and its gate evaluations, nothing else. Readmission can still wait outside both deadlines: the migrations snapshot, the orphan-witness barrier (up to 30 s per peer pull), the recorded-outcome and pending lookups, and every durable record of an outcome, a refusal's included.
- **The window after readmission.** The replica groups are computed once more after the quota wait and primacy is not checked on that result. That window exists on main with or without a wait and is not closed here.
- **Earlier uncertainty.** The recorded-outcome and pending replays reduce it; their absence does not prove every earlier attempt resolved (the earlier-WRITE_STALLED case is untraced). Neither solved nor waived here.
- **DC-210 and DC-211 stay open.** The Today table delete caches and records whatever the shared helper returns, a gate refusal included, so a refused delete is not shown to recover by retrying. A post-wait EPOCH_STALE on a delete carries reject code NONE through the existing outer conversion; it is not claimed to equal the entry check's answer or to correct itself on retry. The wait neither creates nor fixes either; a recovered delete simply does not meet them.
- **Other gates.** A wait for `InvalidSignature` after a leader change, for the KV write and linearizable read gates, and for the ParentDecisions holder check.
- **The time base.** The gate judges expiry by the client's HLC. This ADR keeps that and does not endorse it.
- **Shutdown order.** The order of transport drain and lease-manager abort; the wait is bounded either way.
- **A new end-to-end client-loop test.** None was written. The production frame path on the last attempt is covered, and the existing client retry tests cover the unchanged loop.
- **Behaviour under background load.** The 30-iteration repeat ran the new tests alone: it is repetition evidence, not background-load evidence. The parallel focused set is the only run with other tests alongside.
