# ADR 003: The "Stellarator Model" (Bifurcated Write Path)

## Status
Accepted (amended 2026-04-22)

## Context
Raft is the gold standard for strong consistency, but the Raft leader is the wrong place to stream large Arrow IPC batches, file chunks, or slab payloads. A payload-through-Raft design would collapse throughput and couple every data write to Raft WAL bandwidth.

The earlier version of this ADR correctly chose a bifurcated write path, but it stopped at the architectural headline. The gap-planning cycle exposed the missing load-bearing details:

- how recoverable writes are represented across crashes
- when the leader may acknowledge a write
- whether leader-local durability and remote fan-out are concurrent
- how local persistence failures affect leadership
- how restart replay and idempotency compose with the finalization path

Those details are not implementation trivia. They are the correctness contract of the Stellarator model.

## Decision

Wendelon keeps the bifurcated write architecture, but the contract is now explicit.

1. **Bulk payloads never enter the global Raft WAL.**
   Payload bytes are replicated through the data path. Raft only carries the control-plane records required to finalize visibility and sequencing.

2. **Pending-finalization intent is mandatory for any recoverable branch.**
   Before Wendelon may return a success state that implies recoverability after crash, the leader must durably persist a pending-finalization intent locally. There is no best-effort recovery branch.

3. **Leader-local durable append and remote fan-out start concurrently.**
   If the leader is itself a replica for the write, its local durable append is one leg of the quorum, not a serial precondition before remote replication starts.

4. **Durable quorum is computed across both local and remote durability legs.**
   The quorum aggregator counts:
   - the leader-local durable append, if the leader is in the replica set and the append succeeded
   - remote replica acknowledgements that meet the durability bar

5. **No pure-remote quorum is allowed when the leader is a replica-set member.**
   If the leader is part of the replica set for the write, quorum success requires the leader's own durable append to succeed. The system does not report success based solely on remote replicas while the leader-local durable leg failed.

6. **Write outcomes are explicit and stable.**
   The write path recognizes the following outcome states:
   - `DataBranchPending`
   - `DurableQuorumReached`
   - `FinalizationCommitted`
   - `FinalizationRecoverablePending`
   - `FinalizationTerminalFailure`

7. **Finalization is downstream of durable quorum, not a substitute for it.**
   The leader sends the Raft finalization record only after durable quorum is achieved and pending intent is already durable.

8. **Integrity-threatening local persistence failures degrade or fence leadership.**
   If the leader observes a local persistence failure that makes its durable-leg trustworthiness doubtful, it must fail the write and may step down or fence itself from continued leadership until local storage is revalidated.

9. **Restart replay is part of the design, not a sidecar convenience.**
   Pending-finalization state must replay on restart, and idempotency behavior must be consistent across:
   - in-process retry
   - duplicate client retry
   - crash and restart

## Rationale

- **Scale still requires bifurcation.** The payload path cannot be allowed to collapse into the Raft path.
- **Recoverability requires durable intent, not optimism.** A write that is "recoverable after crash" must leave enough durable state to actually recover after crash.
- **Concurrency is the point.** If the leader waits for its own local durable append before even starting remote fan-out, the architecture has paid for bifurcation without collecting its latency benefit.
- **Leader-local durability is load-bearing when the leader stores the data.** Allowing the leader to report success without its own durable append would create a split-brain durability contract between what the cluster knows and what the leader itself can replay or serve.
- **Explicit outcome states make retry behavior sane.** Clients, replay loops, and operational tooling need a stable vocabulary for "committed", "recoverable but not finalized", and "terminally failed".
- **Fencing is cheaper than pretending a corrupted durable leg is healthy.** Availability without trustworthy persistence is the wrong trade in this part of the system.

## Consequences (The "Negatives" / Rejected Ideas)

- **The write path remains materially more complex than standard Raft replication.** We accept this because standard payload-through-Raft is not the target architecture.
- **Pending-finalization persistence is now on the correctness path.** If that store is unavailable, writes fail rather than silently degrading into a non-recoverable mode.
- **Recoverable-pending outcomes are part of the external contract.** Operators and clients must handle a state where data quorum succeeded but finalization is still replayable work.
- **Leader-local storage health can now affect leadership.** This is intentional: a leader with untrustworthy local durability should not continue to act like a healthy primary for bifurcated writes.
- **Rejected alternative: serial local-durable-then-remote fan-out.** Rejected because it preserves correctness but gives away too much of the latency benefit that justified bifurcation in the first place.
- **Rejected alternative: best-effort pending-finalization recovery.** Rejected because "recoverable after crash" without durable intent is a lie.
- **Rejected alternative: pure-remote quorum while the leader is a replica.** Rejected because it weakens the leader's own recovery and visibility contract.

## References
- ADR 002: Unified Hash Ring Topology
- ADR 007: Raft Implementation Strategy
- ADR 015: Segmented WAL and Snapshot Lifecycle
- ADR 016: Durability Model — Explicit fsync Group Commit
