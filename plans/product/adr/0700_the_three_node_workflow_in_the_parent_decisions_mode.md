# ADR 0700: the three-node workflow in the parent decisions mode

Status: proposed, 2026-11-08 (Claude, Codex). Implementation lands with this plan. It is S6 of ADR 0699, which delivered S1 to S5 and left S6 to a later ADR.

## Why
ADR 0699 built the `ParentDecisions` authority for a disposable three-node development checkpoint. The mode refused ordinary service until its sixth unit: the original three-node workflow test, unweakened, passing in the mode. This ADR is that unit.

**Review record:** `reviews/s6-workflow/` (DESIGN, CODEX-1, the deltas and their rulings, s6-r1 to s6-r4).

## Decision
1. **Acceptance:** `tests/three_node_workflow.rs` runs in the mode, with three fixture changes:
   - a UUIDv7 client identity issued at run time (the mode keys writes by it);
   - the mode's opt-in;
   - a strengthened acknowledgement oracle (below).

   Its bounds, stages and exact-row checks are unchanged.
2. **This is not production readiness.** It is not readiness for five- or seven-node clusters either. Three nodes is the acceptance fixture: nothing below fixes the membership at three.

## Service
- **Opt-in.** `NodeBootstrapConfig::with_parent_decisions_for_disposable_store()` is the only way to select the mode. Bootstrap still refuses it unless the profile allows development keys.
- **Opening.** In production, the mode opens exactly the surfaces it serves: TABLE_WRITE, the three child-write frames, TABLE_QUERY, WRITE, READ and TABLE_DELETE. Every other data frame and stream is refused at dispatch. The test-only opening is removed.
- **Readiness.** The mode's term replaces `ParentDecisionsIncomplete` with the reason the store cannot serve now:
  - its decision signer is bound at its current incarnation;
  - every primary range the current ring gives it is held as its lineage at that incarnation and epoch, and covered by its lease at the current epoch.

  That term is derived from the ring, never from a node count. It sits alongside readiness's other checks, which stay unchanged: the general degraded state, certified-base validation (ADR 0699 S5a), tier-authority readiness and restore validation. Readiness is a revocable observation, not a capability: admission and read authority are still checked at use.

## Writes
- **Client retry.** `WRITE_STALLED` is retried with the same identity and the same budget (3 attempts). An exhaustion after any unresolved attempt reports INDETERMINATE, never not-committed.
- **The decision clock gate.** A leader stamps time-bearing commands only when its clock agrees with its peers' median:
  - q = ⌊n/2⌋+1 from the applied voter configuration; joint configurations are refused;
  - at least q−1 distinct eligible peer samples: voters, not itself, each fresher than 5 s by a monotonic age;
  - the median over ALL eligible fresh samples, each projected forward by its monotonic age, lies within `MAX_OFFSET_MS` of its own clock.

  This is agreement with that peer median, given the participation threshold. It is not proof that each member agrees, nor of objectively correct time.

  Samples come from the requests followers send and from a bounded probe. A follower that has sent the leader nothing for 1 s sends one CUT, with one in flight at most. The maintenance loop owns that probe: stopping the loop cancels and awaits it, and aborting the loop requests its cancellation.

  A single voter's clock is unchecked. That is an explicit limitation, not a claim of validated time.
- **Only Reserve and RecordNoop bear time.** Admission leaves them unstamped (`Unstamped`), and only `Unstamped::stamp`, fed by the gated clock, makes them commands. This enforces the production path; it is not a type-level proof against another caller supplying a raw value. Every other forwarded variant handled by this admission enum needs no time and stays admitted while time is unhealthy. The leader's floor advance is not forwarded, and it uses the same clock gate.
- **Commit timestamps.** A new reservation whose commit HLC is more than `MAX_OFFSET_MS` ahead of the leader's replicated stamp is refused (`CommitFromFuture`, appended).
  - It is checked after the recorded-outcome lookup, so a recorded parent still answers as recorded.
  - It changes nothing when it refuses.
  - It bounds the admission of future physical timestamps only. It does **not** establish that a later accepted write supersedes an earlier one, and latest-by-HLC ordering is unchanged.
  - A client HLC is merged before admission, so a far-future client HLC still advances the coordinator's HLC. That coordinator's later writes are then refused until its clock catches up. This is registered, not fixed.
- **Tombstones.** A delete's commit HLC is minted by `tombstone_hlc`: the winner is merged first, so the tombstone is strictly above the winner's full HLC even on a clock behind it.
- **Leases in the mode.** A holder's leases are the applied lease record: its own grants at the current epoch, unexpired on its clock, whichever leader issued them, with the issuer's signature verified. A lineage already continues across a change of issuer (`admit_lease_grant`), so this removes the window after every failover in which a holder's valid leases stopped counting. A grant after a gap still closes the old lineage and opens a new generation. A parent bound to the old lineage becomes Refused only through an applied Finalize or Resolve that finds it closed, not automatically at the lease's expiry or at the grant.

  Holder admission, repair and readiness keep their lineage, incarnation, sender and plan checks. Today's lease handling is unchanged.

## Clocks and legacy paths in the mode
- The CDC locality heartbeat does not run in the mode, because CDC is not served. Its legacy Finalize entries were what fed the drift monitor stale samples after a failover.
- A legacy pending finalization is refused, never executed as a mode write.
- The drift monitor and its abort are unchanged. With no legacy feed it reports insufficient samples, which is not evidence of a healthy clock and is not part of readiness.

## Reads
A table query refused at admission is answered with a typed terminal, not an unframed close:
- a tier authority that is not ready (`WouldBlock`, or its pending fence) → `BACKPRESSURED`: a bounded retry is permitted, not a promise of recovery;
- any other refusal → `QUERY_EXECUTION_FAILED`.

This closes the escape that ended a query at a reopened member with `early eof`. The denied-query audit and the transport write can still fail first. Unary paths are unchanged.

## The workflow's acknowledgement oracle
A write is acknowledged only when its answer is COMMITTED, with OK or DUPLICATE. Before, any OK counted, and the service answers OK for a write it recorded as refused.

A definite not-committed answer (OK or `WRITE_ATTEMPTS_EXHAUSTED`, with KNOWN_NOT_COMMITTED) is sent again under a new identity, within the same bound. Any other response status fails the workflow. The existing handling of transient transport errors is unchanged: the same identity is retried within the bound.

## Evidence
- **Focused controls for each change**, under `reviews/s6-workflow/`:
  - the gate's 3/5/7 threshold table;
  - Reserve's bound at its edge;
  - heartbeat suppression in the mode and Today;
  - owned-probe stop and drop;
  - the time-bearing selection;
  - the applied lease source and grant transitions;
  - admission-refusal classification and a held tier authority;
  - a reopened member's queries;
  - failover availability.

  Widened focused runs: 759/759 (r3) and 768/769 (r4, a fence later met: 252/252 on the exact tree).
- **The workflow, instrumented** (s6-inst3): base 2aacd38cd, landing r4, the diagnostic delta, composed tree `92bbfa32`.
  - **Passed** in 14.0 s through every stage: tiering and reclaim, follower restart and catch-up, failover, the old leader down and back, and a last write.
  - No write was resent; no diagnostic fired.
  - Every node closed by a graceful shutdown within its bound. The run was shorter than the maintenance drain bound, so no forced abort occurred.
- Earlier instrumented runs located the two blockers fixed above: s6-inst1 found the lease issuer filter after a failover, and s6-inst2 the bare close at a reopened member.

## Not established
- **Open defect candidates:**
  - the LeaseMissing stall of a first write right after `ready()` with no leader change (`racing_attempts_and_retries_follow_the_recorded_claim`), still open;
  - the availability effect of a far-future client HLC (above).
- **Registered candidates, from source observation only and not reproduced; not part of this ADR:**
  - the bare close on an admission refusal of the QUERY, BULK_WRITE and STREAM_FIN streams (reachable in Today);
  - audit-error framing before a terminal.
- **Carried from ADR 0699:**
  - the all-member reclamation barrier (it stalls while any member is absent);
  - LSM certified elision;
  - tiered cleanup containment;
  - everything in 0699's "Not established".
- **§8 is still FAILED and open.**
- Write latency, larger clusters and production activation are not established.
