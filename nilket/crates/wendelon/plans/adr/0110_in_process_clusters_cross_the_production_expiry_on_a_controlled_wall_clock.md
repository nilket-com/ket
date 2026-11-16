# ADR 0110 - In-process clusters cross the production expiry on a controlled wall clock

## Status
Accepted for implementation, 2026-11-16 (DST stage 1). The direction, the protocol and each correction were reviewed by Codex 01a1e607, 01a1e616, 01a1e61e, 01a1e637, 01a1e648, 01a1e655 and 01a1e6c6. Not delivered.

## Context
- **The expiry is 66 minutes of wall time.** A ParentDecisions key retires only when the applied floor passes `issued + E`, where E = `DECISION_EXPIRY_MS` = 3,960,000 ms. Certification over those keys, and so a replacement's later generation and activation, waits that long.
- **In-process tests shortcut it.** They commit an `AdvanceFloor` whose stamp is E ahead (`jump_floor`). That moves the ring clock and the floor in one entry, and bypasses the clock gate, the lease renewals and the maintenance that real time would exercise.
- **The replacement workflow needs two expiry intervals:** 132 minutes of protocol wall time. No natural-time run of it was measured here.
- **The wall clock is already injected.** It is the `PhysicalClock` trait, `SystemClock` in production, read by the HLC, the decision clock gate, lease stamps and maintenance. Timers and deadlines are Tokio monotonic time, so a wall step does not shorten a lease's renewal interval or a request's bound.
- **Two wall reads bypassed it:** the ControlOnly preparation's `sent_ms` and the recovery driver's candidate `now_ms`.

## Decision

### D1. One cluster, one test time (`node/test_time.rs`, `#[cfg(test)]`)
- **`TestWallClock`:** real `SystemTime` plus a forward-only offset. It never reads below a value it returned; `advance(d)` returns W, strictly after every earlier reading.
- **`ClusterAbortLatch`:** the cluster's `AbortHandle`. It records the reason, then panics in the aborting task, so the process survives and the test fails on the latch.
- **`PassWake`:** one per owner, per node, for the maintenance loop and the lease schedule. A request returns a ticket and coalesces. A pass answers only requests made before it BEGAN. `completed(ticket, by)` refuses an answer observed after `by`, a real monotonic bound.
- **`TestTime`:** the three together, kept by node id across lifetimes. `NodeBootstrapConfig` carries `#[cfg(test)] test_time: Option<TestTime>`, following the `startup_probe` pattern. It is not a daemon config or CLI field.

### D2. The production seams (ordinary builds keep the production values)

| file | ordinary (non-test) build | `cfg(test)` build |
|---|---|---|
| `node.rs` `node_clock(cfg)` | `SystemClock` | the cluster's clock if `test_time` is set |
| `node.rs` `node_abort(cfg)` | `ProcessAbort` | the latch if set |
| `node.rs` `PassWakeHandle` | `()` | `Option<Arc<PassWake>>` |
| `node.rs` `pass_requested` | `pending()` forever (a `select!` branch that never fires) | the owner's wake |
| `node.rs` `pass_begin` / `pass_finish` / `pass_after` | no-ops | ticket accounting; a one-shot hold after a pass, for the stop-versus-wake control |
| maintenance loop | stop stays first in its biased `select!`; the wake branch follows the ticker | — |
| `network/data_server.rs` | `spawn_lease_manager` keeps its public signature and delegates to `pub(crate) spawn_lease_manager_with_wake(…, ())`; the schedule's wake branch is `pending()` | the owner's wake |
| `node/replacement_preparation.rs` `sent_ms` | `node_clock(cfg).now_ms()` = `SystemClock`, the same conversion | the cluster's clock |
| `node/replacement_recovery.rs` candidate `now_ms` | `RecoveryDriver.clock` = `SystemClock` | the cluster's clock |
| `network/decision_proposals.rs` | — | `clock_samples_for_test` (read only) and `test_directed_sample`, which IS `cut()`: the ordinary authenticated CUT at a test-chosen time |
| `config.rs` | — | `test_time: None` |

The lease schedule has no stop branch. It ends when its epoch watch closes, or when shutdown aborts and joins it.

### D3. The advancer (`parent_decisions_cluster_tests/time_advance.rs`)
Each step:
1. **Before the step:** every voter with no current Full owner (a ControlOnly replacement) has its sample at the leader observed past freshness, by the gate's own predicate.
2. **Size the step:** `s ≤ min(TTL/2, every live Full lease's expiry − now − (TTL/3 + 500 ms))`. A reached target is completion, with no step.
3. **Step:** `W = advance(s)`.
4. **Wake:** a directed sample from each Full follower that lacks a post-step sample; then fresh maintenance tickets, then lease tickets, for each current Full lifetime.
5. **Judge restoration,** observed no later than the real lease slack and the step deadline:
   - every other voter's FRESH sample is `≥ W`; at least ⌊n/2⌋ such samples; `clock_gate` Ok;
   - the applied decision clock is `≥ W`;
   - for every live Full lease, an applied renewal witness: an `AcquireLeases` entry past the pre-step index, with leader stamp `≥ W`, the same tenure, continuing the lease, equal to the applied lease, expiring past `W + margin`;
   - no Full lineage changed;
   - the latch is empty, and every Full maintenance owner is running.
6. **Scan:** every applied generation event (Target, Report, Commit), contiguously (a gap is INCOMPLETE EVIDENCE).

**Never:** a cache edit, a sample edit, a floor shortcut, or a changed TTL, E, skew bound or authority rule.

### D4. The pilot (`replacement_activation_tests.rs`, `#[ignore]`)
The ADR 0108 activation handoff with the production E crossed twice. One 780 s workflow deadline is judged at observation.
1. **Phase 1:** E with all three nodes Full; generation 1 through the real floor.
2. **The loss:** the ControlOnly replacement on the same time; its recovery recorded `(G, K)` with its activation gate CLOSED; row 4 written as the designated reservation.
3. **Phase 2:** E with the replacement owner-less.
4. **Still gated:** wait for a certificate past G that covers row 4's reservation, with its exact commit scanned and the replacement's own report between target and commit; check that the reservation was erased.
5. **The gate opens:** activation at that certificate or a successor.
6. **Handover:** `hand_over`; the Full start and the Full restart each serve exactly rows 1–4.
7. **Final checks:** every shutdown checked; the latch empty.

After a failure, cleanup is bounded, and each outcome is printed.

### D5. Development and test builds compile `ed25519-dalek` and `curve25519-dalek` at `opt-level = 3`
- A **repository-wide** dev/test profile change (`nilket/Cargo.toml`), following the scrypt/salsa20 precedent. Debug assertions and overflow checks are unchanged; no dependency or lockfile change.
- **Why:** a follower's renewal pass verifies every granted lease's signature. Unoptimised, that was about 200 ms of a 3 s TTL per pass (an inference from diagnostics).
- **Measured on the restored-steps control,** a changed-profile sample:
  - restoration went from 3,502 ms total and 253 ms maximum, to 93 ms and 7.1 ms, over 14 steps;
  - the lease phase went from 3,459 ms to 66 ms.
- This is test-build policy, not production performance evidence. It changes every Ed25519 timing in the workspace's tests.

## What this is, and is not
- **It is** controlled wall-time protocol testing with real monotonic timers and real I/O.
- **It is NOT** deterministic or replayable simulation: scheduling stays nondeterministic, and a failure is not replayable from a seed.
- **Coverage limits:**
  - writes use hand-minted identities through the in-process `write_at`, not a public client;
  - ParentDecisions gives the drift monitor no samples, so a missing abort is no positive drift coverage; the latch is armed for any abort;
  - clock agreement is the gate's healthy subset;
  - the handover is in-process (`hand_over` and a Full reopen), not a daemon exiting 75; the real-daemon records cover that.

## Validation
- **Component controls** (development lane):
  - the test clock;
  - the latch;
  - wake ordering, coalescing, the late-answer refusal, and stop-before-wake;
  - the directed sample;
  - sizing, the age-out boundary, and the renewal-witness lookalikes;
  - the late-restoration refusal and the workflow-deadline refusal;
  - the reached target;
  - the contiguous scan;
  - certificate selection and the evidence chain;
  - restored steps in a real three-node cluster.
- **Pilots** (one execution each, ruled):
  1. **FAILED:** my own oracle asserted gid ≥ 2 on an activation that was correctly at the current gid 1, and the gate ordering was wrong. Corrected into D4's gate.
  2. **FAILED:** the full certification chain passed, then `hand_over` refused "the instance was never granted". That exact error was independently reproduced by DC-257, whose mechanism ADR 0109 corrected. Pilot 2's own path is unestablished, and a later passing run is not retroactive proof of it.
  3. **PASSED** on `fdbb1dd5` (ADR 0109 landed):
     - 61.2 s of real time, 5,273 restored steps, about 132 min of wall time;
     - certificate gid 3 (target 18683, the replacement's report 18686, commit 18688), covering reservation 10709;
     - the handover, then the Full start and restart with rows 1–4 exact; every shutdown checked.
- **Delivery checks** (chosen on the candidate): the ordinary library, both architecture targets, and the short real three-daemon run, because these files touch shared startup, runtime and request paths.

## Consequences
- **Production code carries inert seams:** a `select!` branch that is pending forever, no-op pass hooks, and two clock reads routed through `node_clock` (`SystemClock`).
- **The test harness owns a protocol** that must change if the clock gate, lease renewal or certification rules change. Its judges name the source rules they mirror.
- **The pilot is ignored by default.** It runs on purpose, under an owner and a deadline.

## Not decided here
- Deterministic or replayable simulation, simulated transport time, and further pilots.
- Positive drift-monitor coverage (the Today-mode candidate DC-256).
