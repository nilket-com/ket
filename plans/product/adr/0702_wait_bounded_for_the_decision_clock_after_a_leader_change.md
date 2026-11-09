# ADR 0702: wait, bounded, for the decision clock after a leader change

Status: proposed, 2026-11-09 (Claude, Codex). Implementation lands with this plan. Direction: Codex 01a1bf99. Review record: `reviews/lease-missing-startup/` (GATE-FAILURE-DIAGNOSIS, CODEX-GATE-DIAGNOSIS) and `reviews/clock-admission-wait/`.

## Why
`three_node_workflow` can fail right after a leader change. The first write's Reserve is refused `ClockUnhealthy` by the new leader's decision clock gate (ADR 0700 delta 1). The client's budget (3 attempts, 20 ms backoff) then ends as INDETERMINATE, and the workflow's oracle correctly fails it. Under one experiment there were 3/5 observed failures on main (ADR 0700) and 2/5 with ADR 0701; no new failure mechanism was identified. It is a readiness blocker.

**The mechanism:**
- The gate needs at least q−1 fresh (≤ 5 s) distinct voter samples. A stopped node stays a voter, so q = 2 and the new leader needs one fresh sample from the surviving follower.
- **A forwarded request contributes its sender's sample before admission** (control_server `handle_decision_propose` → `observe_sender_clock`). That does not guarantee a healthy gate: the sender may not be a current voter, its clock may disagree, and larger memberships need more samples.
- The trace explains the observed failure. There, the new leader coordinated the write itself and needed a fresh sample from the surviving follower, which comes from that follower's incoming decision traffic, including its clock probe: a CUT sent on its 500 ms maintenance tick when it has sent the current leader nothing for 1 s. That sample arrives after the client's budget is spent.

## Decision
**Admission of a time-bearing command waits, bounded, for the gate to become healthy, using the existing follower probe.** There is no new RPC and no leader-initiated solicitation.

- **Where:** `DecisionProposals::propose_as`, only for `Admitted::Unstamped` (Reserve and RecordNoop). Ready operations, and the leader's floor advance, keep today's immediate gate and acquire no delay.
- **The wait:**
  - one absolute monotonic deadline, now + `CLOCK_ADMISSION_WAIT` (= `CLOCK_PROBE_TIMEOUT`, 2 s), never renewed on a wake;
  - it bounds the **whole evaluation**, every gate evaluation (whose state read can block under contention) and the final leadership check included. On expiry the gate's own refusal stands, and a stamp ready only at the deadline does not win. It bounds admission, not the Raft commit that follows;
  - no lock is held while waiting;
  - it wakes when a sample is recorded (a `Notify` signalled by `DecisionClockSamples::record`; the notification is armed before each evaluation, so none is missed) or at the deadline;
  - each wake re-evaluates the unchanged gate against the current applied membership, the current fresh samples and the current local clock.
- **Outcome:**
  - **healthy** → leadership is rechecked before stamping; if this node is no longer leader the answer is NotLeader and nothing is proposed;
  - **any refusal other than ClockUnhealthy** returns at once;
  - **the deadline, still unhealthy** (no sample, or a real disagreement) → the existing `ClockUnhealthy` refusal, with nothing proposed. Fail-closed is preserved.
- **Threshold, freshness, median bound and membership rule are unchanged.** No client budget, retry, TTL or readiness change.
- **Lifecycle:** the wait lives in the request handler's own future, so dropping that future cancels its wait. That alone does not establish that a remote caller's cancellation or a connection closure drops an already-dispatched handler; the enforced deadline bounds a handler that stays alive. Nothing is spawned per request.

## Deadline and scheduling trace (why the bound fits)
- **A forwarded Reserve** (`propose` → `forward` → `round_trip`) has no per-request timeout of its own. The coordinating node simply awaits the response, so a leader-side wait of ≤ 2 s delays it and does not break it.
- **The client** sets only the caller's own deadline per attempt (`prepare_mutation_attempt_deadline`). The workflow passes none, and a caller deadline shorter than the wait still ends the caller's attempt as it does today.
- **The transport:** QUIC keep-alive with an idle timeout well above 2 s; the in-process test transport has none.
- **The follower's probe** runs in its own maintenance loop (an owned task on the follower). On the leader, its CUT is handled in its own request task, and `observe_sender_clock` takes the sample map's mutex only briefly. A waiting admission holds no lock, so probes and their recording progress while it waits.

## Controls
- **The wait helper, at the unit level with paused time:**
  - a sample arriving during the wait → healthy, stamped from the re-evaluated clock;
  - no sample → `ClockUnhealthy` exactly at the deadline;
  - a real skew that persists → `ClockUnhealthy` at the deadline;
  - a non-clock refusal → immediate;
  - a leadership loss before stamping → NotLeader, nothing proposed;
  - a gate held past the deadline, then succeeding late or exactly at the deadline → `ClockUnhealthy` at the deadline, no stamp;
  - a leadership check held past the deadline after a healthy gate → `ClockUnhealthy` at the deadline, no stamp.
- **The workflow oracle is unchanged.** One predeclared bounded loaded batch: `three_node_workflow` ×10 under the same background load as the diagnosis, every outcome reported, no outer retry.

## Evidence
- **Focused controls** (paused time): sample arrival, no sample, repeated disagreeing samples, a non-clock refusal, leadership loss, a gate held past the deadline (late and exactly at the deadline), and a leadership check held past the deadline. All pass.
- **The predeclared loaded batch:**
  - **Setup:** the base plus this source plus a scratch print-only diagnostic kept out of this source, run as `three_node_workflow` ×10 under the background load the failure was diagnosed with.
  - **Result: 10/10 passed.**
  - **Diagnostic:** zero `reserve: Refused(ClockUnhealthy)` (the same load before this ADR logged 8 to 9 per five iterations); the background load passed 661/661.
  - Bounded evidence, not a statistical reliability guarantee.

## Not established
- A statistical reliability guarantee: a passing bounded batch is bounded evidence.
- Liveness when no voter sample can arrive within the bound; it still refuses, as before.
- Any change to the gate's threshold or semantics.
