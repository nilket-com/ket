# ADR 0707: a stopping node stops waiting for its maintenance proposals

Status: proposed, 2026-11-10 (Claude, Codex). Design cleared (Codex 01a1c5e2). Implementation lands with this plan. Closes DC-203 (tiered-arc alias DC-198) for the demonstrated mechanism once its implementation and evidence are reviewed. The attribution of the historical failure in adr0706-gate1 stays unestablished. Review record: `reviews/dc195-layout-collision/` (DC-203 entries, `DC203-MECHANISM.md`).

## Why
- **DC-203 (availability; fails closed):** in adr0706-gate1, the three-node workflow's final stop returned `TimedOut` ("decision maintenance did not stop within the lifecycle drain bound; aborted"). It was observed once and never in 13 diagnostic attempts.
- **Mechanism (source; reproduced on main 6b380bee2):**
  - The decision maintenance loop (DM, `node.rs`) checks its stop only between ticks.
  - Five of its steps wait for a Raft answer with no bound: `ensure_signer_once`, `advance_floor_once`, `resolve_closed_once`, `generation_step`, and the certification step's Report. They go through `DecisionProposals::commit` (`raft.client_write`), or, on a follower, through `forward` / `round_trip` (connect, open, write, read: none bounded).
  - openraft answers a `client_write` only when its entry is applied, when a conflicting leader truncates it, or when RaftCore stops, and a leader never steps down by itself.
  - Since ADR 0706, `raft.shutdown()` comes after the lifecycle drain. So a leader that has lost its last reachable voter holds the drain to its 15 s bound, and the close returns `TimedOut`.
  - A deterministic control showed it on main (dc203-probe-r1): one follower closed and the other unpublished, then the leader's log held a maintenance entry past its applied index 86 ms later, and the leader's close took 15.007 s and returned that error. `CLEAN_SHUTDOWN` was still written, because the aborted step held no permit.
- **Not only a fixture effect:** any orderly close of a leader that has lost quorum is exposed, for example when a cluster's members are stopped one after another.

## Decision
**Decision maintenance's own proposals wait for their answer only until the node stops. A stopped wait is a local fact about the caller, never evidence about the proposal: the entry may already be committed, or may commit later, and Raft alone decides it.**

1. **A local, maintenance-only result.**
   - The DM's proposals go through an explicit maintenance capability that carries the DM stop and returns `MaintenanceProposalResult::{Stopped, Response(ProposalResponse)}`.
   - `Stopped` is local and never serialized. `ProposalResponse`, a wire type, is unchanged.
   - The stop is not attached to the shared `DecisionProposals`. Proposals from client RPCs, and the leader's handling of forwarded proposals, keep their semantics; the maintenance stop never cancels them.
2. **Where the wait is cut.**
   - The capability selects, biased towards the stop, around the whole proposal future: the leader's admission wait and `client_write`, and the follower's connect, open, write and read. No transport stage stays outside it.
   - A stop already signalled is seen before anything is polled or submitted.
   - Cancellation is never selected around a whole step or its durable work. For the certification step, the cut point is its Report wait, after its local durable staging; the step then returns normally, so its permit completes through the ordinary path.
   - No cancellation handler completes a permit or writes the clean record.
3. **No fresh proposal after the stop.**
   - The loop's select is biased towards the stop.
   - Once the stop is set, no later substep of the admitted tick emits a proposal; `resolve_closed_once` ends its remaining loop.
   - The admitted tick finishes its remaining bounded local work under the existing drain semantics (ADR 0699 S5a, ADR 0706).
4. **Why stopping the wait is safe: each command's replay basis.** Each caller derives its command from applied state on every tick, and the reducer makes a late, duplicate or re-derived copy harmless:
   - **Common ground.** Raft has no log-level dedup: every copy commits as its own entry, and the reducer decides its effect. The argument rests on the specific stale and duplicate checks listed below, each of which refuses before changing anything, and on the maintained retained-bytes invariant (`consensus/decisions.rs`, `DecisionState::validate`). It does not rest on a blanket claim that every refusal leaves state unchanged: DC-205 is a known counterexample (a `CounterOverflow` refusal in `commit_generation` after its removals), reachable only if that invariant is already broken, and it stays a separate open exception. An entry from an old term that survives a leader change is applied before anything the new leader appends, and every check below runs again at apply time.
   - **RegisterIncarnation:** derived only while the applied registry has no current incarnation for this node. An exact replay returns `Success` with no change (`raft_storage.rs`, `apply_register_incarnation`, the accepted set). If another incarnation got there first, the copy is refused `StaleSupersede`.
   - **BindSigner:** derived only while this incarnation's key is unbound. A second copy returns `SignerBound` and inserts nothing. A different key gets `SignerRebind`, and a non-current incarnation gets `SignerNotCurrent` (`decisions.rs`, `bind_signer`).
   - **AdvanceFloor:** derived only while the floor would rise. A floor at or below the current one is refused `FloorNotAdvanced`, with the committed clock untouched. A late copy with a higher floor stays valid, because its floor is its own stamp minus the expiry (`decisions.rs`, `advance_floor`).
   - **Resolve:** derived for non-terminal parents with a closed lineage, whose coordinator range this node holds. A terminal parent returns its existing result unchanged. A parent no longer open is concluded only if a lineage is still closed (otherwise `StillOpen`). A digest mismatch or an absent key is refused (`decisions.rs`, `resolve`).
   - **ProposeTarget:** derived with `gid = next_gid` while no target is pending. A second copy is refused `StaleGeneration`, because `next_gid` has advanced; it never replaces the recorded target or its reports (`generations.rs`, `propose_target`).
   - **CommitGeneration:** derived when every member has reported. A second copy finds nothing pending, and a copy whose target was replaced fails the gid check: both `StaleGeneration` (`generations.rs`, `commit_generation`).
   - **Report:** skipped while the applied state already holds this node's report. Its body comes from the durable staged metadata. An identical copy returns `ReportRecorded` and inserts nothing (the comparison is on the body). A copy after the target was replaced or committed is refused `StaleGeneration`, and a different body from the same reporter `ReportMismatch` (`generations.rs`, `record_report`).
   - **Not the argument:** an ignored answer, by itself.
   - **The Report at reopen.** The staged candidate, and with it the report's body, is durable and reconstructible before the proposal. The signed report itself is built in memory (`evidence::sign`) just before it is proposed and is not persisted: what makes a report proposed again safe is that re-signing an identical body gives a report the reducer compares by body. A stopped wait does not certify that the report failed.
     - At reopen, a staged candidate for the pending gid is restaged only if it is exactly the stored one (`storage/index/certified_base.rs`, `restage_base`). A different candidate for the same gid is refused (`stage_base`). So a report proposed again equals the committed one.
     - A node that never learned the answer needs nothing more: the leader consumes reports from applied state, and the node catches up through `activate_committed`, which matches gid, digest, predecessor and chunks and fails closed otherwise.
   - **Dropping the wait mid-flight:** the leader's `propose_as` writes nothing before an await and holds no lock across one, and an answer for a dropped receiver is discarded by openraft. The follower's `round_trip` records `last_sent` before its awaits. A stopped forward therefore leaves that one timestamp, which only delays the stopping node's own clock probe, already stopped.
5. **As built:** the capability is `MaintenanceProposals` (`network/decision_proposals.rs`), built in the DM loop from the shared proposals and the DM stop. `MaintenanceProposalResult` is not serialized. The certification step takes the capability, and its Report is the step's only cut point. The controls use two `cfg(test)` holds: `PauseStage::CertificationBeforeReport`, and a hold on the leader's handling of one forwarded proposal. Neither changes production behaviour, and neither is the stop.
6. **Unchanged:** the 15 s lifecycle drain bound; the abort-and-join past it, with its `TimedOut` result; the clean-record semantics, including that a permit-free forced abort still records `CLEAN_SHUTDOWN` (the error reports the exhausted bound, the marker reports local recovery state); the interrupted-permit refusal. No timeout is shortened.

## Acceptance (finite)
1. **A leader without quorum closes promptly:**
   - Setup: one follower closed, the other unpublished but running, and a pending maintenance proposal observed at the leader (last log index past applied).
   - The leader's close returns `Ok` well within the bound and records `CLEAN_SHUTDOWN`.
   - With quorum restored, either Raft outcome is accepted (the old entry applied, or truncated and safely derived again), and the state is exact with no duplicate effect.
   - Fails on main (dc203-probe-r1: 15 s, `TimedOut`).
2. **Certification held at its Report wait, after durable staging, without quorum:** the close returns `Ok` and is clean, and the permit is completed by the step's own return. After reopen with quorum, the report is committed exactly once, or proposed again from the staged candidate.
3. **A follower's forward, blocked on an in-flight response, is stopped:** the close returns `Ok` promptly.
4. **A ready stop is seen before a new tick or a new proposal** (an admission check), and an ordinary RPC proposal is not cancelled by the maintenance stop.
5. **The workflow:** `three_nodes_keep_every_acknowledged_row_through_tiering_restart_and_failover` ≥5 runs under load at the final tip, the rate reported; then one checkpoint.

## Not established
- That the gate1 failure was this mechanism; this ADR shows only that the mechanism exists.
- The other unbounded awaits of a DM tick, unchanged, as is the drain bound's adequacy for them:
  - local index and lock waits;
  - `validate_certified_base`'s read cut, which on a follower is an unbounded `round_trip` to the leader. It is not a proposal, and it runs only until the base is validated after a start.
- DC-201 (open after its bounded triage), §8, and the other registered limits.
