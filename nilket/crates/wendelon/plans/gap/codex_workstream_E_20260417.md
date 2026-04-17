# Codex Workstream E: Tiered Storage Closure

Date: 2026-04-17

Derived from:
- `plans/gap/codex_execution_20260417.md`

## Objective

Turn L2 from an implemented subsystem into a production-integrated one that converges correctly across leader, follower, restart, and failure scenarios.

This workstream exists because tiering is currently one of the largest gaps between subsystem implementation and product readiness.

## In Scope

- Enable L2 configuration on the public config path.
- Start the real storage tiering sweeper in production bootstrap.
- Preserve follower convergence with correct local cleanup behavior.
- Pending-upload tracking.
- Orphan reconciliation.
- Remote reclaim/object lifecycle management.
- L2 read/write mechanics improvements.
  - range reads
  - object-store interface evolution if needed
- Crash and restart convergence for tier pivots and compaction.

## Out Of Scope For This Pass

- Full multi-tenant L2 routing model.
- Pure performance tuning unrelated to correctness or convergence.
- Long-term archival policy work beyond the immediate production-integrated tiering lifecycle.

## Primary Deliverables

- L2 can be enabled from config and exercised through the shipped daemon.
- Tier pivots converge across cluster roles and restart paths.
- Remote leak behavior is actively managed rather than accepted.
- The follower disk leak is closed on the product path.

## Key Interfaces And Dependencies

- Depends on Workstream A for public runtime wiring and validation.
- Depends materially on Workstream B for early tier-observer correctness.
- Must coordinate with:
  - Workstream D on recovery and crash invariants
  - Workstream C on L2 encryption-at-rest behavior
  - Workstream F on memory-bounded cold-read and tier fault testing

## First-Pass Sequencing

1. Enable public L2 configuration and production sweeper wiring.
2. Confirm follower cleanup path is correct on stock bootstrap.
3. Add pending-upload tracking and orphan reconciliation direction.
4. Add remote object lifecycle management.
5. Resolve range-read / interface-evolution direction.
6. Add crash/restart validation for tier pivots and compaction.

## Validation Shape

- End-to-end tiering on real nodes.
- Follower receives tiered state and converges locally.
- Crash during tier pivot and restart convergence.
- Orphan detection/reconciliation behavior.
- L2 reads validated without assuming whole-object fetch forever.

## Exit Criteria

- L2 is configurable and operational on the shipped daemon path.
- Follower disk leak is closed.
- Crash/retry behavior converges without routine orphan accumulation.
- Tiering correctness is validated on the real runtime path.

## Open Questions

- Should range-read support be treated as required for first L2 production integration, or can correctness closure land first with the interface change immediately after?
- How should pending-upload tracking be represented: Raft-managed state, local journal, or a hybrid?
- How much remote object reclaim should land in this workstream versus a later retention/archival policy pass?
