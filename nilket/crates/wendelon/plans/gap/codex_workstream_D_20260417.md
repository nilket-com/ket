# Codex Workstream D: Durability Lifecycle Closure

Date: 2026-04-17

Derived from:
- `plans/gap/codex_execution_20260417.md`

## Objective

Finish the storage and control-plane lifecycle so durability claims hold during steady-state operation, not only during recovery or unit tests.

This workstream closes the gap between "recovery code can consume artifacts" and "runtime actually emits and manages those artifacts correctly over time."

## In Scope

- Live snapshot persistence.
- Runtime checkpoint emission.
- Durable WAL compaction/purge.
- Background scrub.
- Corruption reporting and repair hooks.
- Large snapshot handling strategy.
  - likely streamed or chunked rather than whole-buffer

## Out Of Scope For This Pass

- Full L2 lifecycle closure outside shared recovery invariants.
- Broad performance tuning except where required to make durability mechanisms viable.
- Full operational tooling around recovery workflows.

## Primary Deliverables

- Snapshots and checkpoints are emitted during steady-state runtime.
- WAL growth is governed by a real compaction lifecycle.
- Integrity verification is proactive rather than purely opportunistic.
- Large-snapshot handling stops assuming whole-buffer semantics everywhere.

## Key Interfaces And Dependencies

- Depends on Workstream A for real-path validation.
- Coordinates closely with Workstream C on encrypted artifact handling.
- Coordinates closely with Workstream E on shared recovery and restart semantics.
- Feeds Workstream F by providing more realistic restart and crash surfaces for fault injection.

## First-Pass Sequencing

1. Land live snapshot persistence.
2. Land runtime checkpoint emission.
3. Implement durable WAL compaction/purge.
4. Define and implement large-snapshot handling direction.
5. Add background scrub/reporting hooks.
6. Add restart/crash validation coverage against steady-state artifacts.

## Validation Shape

- Restart from steady-state snapshot plus WAL tail.
- Crash after artifact creation and restart recovery.
- Long-running behavior showing WAL growth is bounded.
- Corruption-detection tests that extend beyond replay-only checks.

## Exit Criteria

- Runtime emits the artifacts that recovery depends on.
- WAL compaction/purge is durably real, not in-memory only.
- Restart and recovery validation uses steady-state-generated artifacts.
- Integrity verification has a proactive runtime component.

## Open Questions

- Should checkpoint emission be tied to snapshot boundaries first, or should it exist as an independent periodic mechanism?
- How much repair automation belongs in this workstream versus later operational tooling?
- Is large-snapshot chunking a hard requirement for first pilot readiness, or can it land as a bounded follow-up if artifact persistence closes first?
