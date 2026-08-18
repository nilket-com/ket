# Wendelon ADR 086: Fail-Closed Orphan-Witness Recovery Write Admission

## Status

Accepted 2026-08-18. Amended the same day to bound peer witness pulls.

## Context

Fresh-leader writes must recover orphan witnesses from peer data nodes before
admitting a local cache miss as a new write. Otherwise a retry whose first
attempt was finalized remotely can be accepted again as fresh, even though the
peer witness is durable and the operation is a duplicate.

The recovery barrier used `Ok(())` for both of these states:

1. recovery for the current leadership stamp completed; and
2. leadership could not be confirmed, so no recovery sweep ran.

The write caller interpreted both states as a satisfied barrier. The second
state therefore allowed fresh-write admission while the witness remained only
on a peer. The original acceptance failure was intermittent and has not been
reproduced; the contract defect is independent of reproducing that window.

## Decision

1. `ensure_orphan_witnesses_recovered_for_leadership` returns `Ok(true)` only
   when the current leadership stamp is confirmed and its witness recovery is
   complete. It returns `Ok(false)` when leadership is unavailable at either
   leadership check.
2. Both bifurcated write paths fail closed on `Ok(false)` and return
   `WriteStatus::NOT_LEADER`. They must not consult a cache miss and admit a
   fresh write while the recovery barrier is unresolved.
3. The pending-finalization reconciler defers its pass on `Ok(false)`. Its
   zero-count result means that reconciliation was not attempted, not that the
   backlog was drained.
4. The leadership-unconfirmed counter remains a permanent diagnostic. The
   duplicate-write acceptance includes the counter delta in its failure
   message. That delta is attributable only in a harness that serializes tests
   within the binary; concurrent nextest tests share the process-global
   counter.
5. Each peer witness pull is bounded by 30 seconds, the same bound used for
   mutation progress stalls. The bound covers connection acquisition, stream
   setup, request transmission, and response framing. A timeout returns an
   `io::ErrorKind::TimedOut` error and fails the recovery barrier; it never
   falls through to fresh-write admission.

## Rationale

Failing closed is the only safe interpretation of an unconfirmed leadership
barrier. Returning `NOT_LEADER` is already a retryable write outcome and keeps
the uncertainty visible to the caller without inventing a new wire status.
The reconciler has no write-admission decision to return, so deferring its pass
preserves the existing caller contract while avoiding the false implication
that reconciliation completed.

The boolean result makes the two semantic states explicit at every call site,
and the deterministic test override exercises the unsafe branch directly. The
acceptance probe remains useful for determining whether the original rare
full-workspace failure ever enters this branch, but reproduction is not a
precondition for closing the unsafe contract.

The peer-pull bound closes the separate liveness gap in the recovery function.
It uses the existing mutation stall policy rather than introducing a second
unrelated timeout authority, and applies to the whole remote operation so a
stalled connection or response cannot hold the leadership barrier indefinitely.

## Consequences (The "Negatives")

Writes can receive `NOT_LEADER` while a node's leadership view is temporarily
unavailable, even if that node is in the process of becoming leader. Clients
must retry through their normal not-leader path. Pending-finalization
reconciliation may also be delayed until leadership is confirmed.

A slow or wedged peer now causes a timed recovery error after 30 seconds. The
write fails closed and the reconciler can retry on a later pass; the timeout is
not treated as evidence that the peer has no witness.

The process-global diagnostic counter is not suitable for per-test attribution
under concurrently running nextest tests. It remains useful as an aggregate
signal, while the acceptance assertion's delta is meaningful only under the
serialized cargo-cgroup harness.

## Open Questions

Whether the rare full-workspace acceptance failure was caused by this exact
leadership-unconfirmed window remains unproven. Future failures now carry the
probe result needed to distinguish that cause from a failure inside the peer
import path; a wedged peer is separately bounded and named as a timeout.
