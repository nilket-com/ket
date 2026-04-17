# Codex Workstream B: Rapid Correctness Fixes

Date: 2026-04-17

Derived from:
- `plans/gap/codex_execution_20260417.md`

## Objective

Land the smallest high-severity fixes early, before they disappear into larger design-heavy phases.

This workstream captures the defects that are both important and relatively contained: admission mistakes, missing observer wiring, obvious operator footguns, and immediately actionable bounding fixes.

## In Scope

- Join admission binding.
  - membership identity must be bound to authenticated transport identity
- Production `TierObserver` wiring.
  - follower-side local cleanup must actually happen on the stock bootstrap path
- Dev-secret hygiene.
  - warnings or sharper separation for deterministic dev/operator-facing example material
- Immediate resource-bounding fixes where implementation shape is already obvious.
  - idempotency TTL/eviction
  - basic per-peer or per-connection concurrency caps

## Out Of Scope For This Pass

- Full authorization model.
- Full encryption-at-rest design and implementation.
- Full backpressure framework.
- Full tiering crash-atomicity closure.

## Primary Deliverables

- Join path no longer trusts payload identity over authenticated peer identity.
- Production bootstrap attaches the tier observer needed for follower cleanup.
- Operator-facing example/config flows no longer silently encourage deterministic signing material.
- A few unambiguous unbounded-state footguns are reduced early.

## Key Interfaces And Dependencies

- Depends on Workstream A for best validation coverage, though some fixes can be prepared in parallel.
- Feeds into:
  - Workstream C by closing the most obvious admission defect first
  - Workstream E by making follower cleanup real before broader L2 closure
  - Workstream F by taking immediate resource-bound wins early

## First-Pass Sequencing

1. Fix join admission binding.
2. Wire production tier observer.
3. Add dev-secret hygiene warning/isolation.
4. Add first-pass idempotency lifecycle control.
5. Add minimal concurrency caps where clearly missing.

## Validation Shape

- Targeted unit/integration tests for each fix.
- Join admission fix validated against authenticated peer mismatch.
- Tier observer fix validated through runtime path where possible.
- Resource-bound fixes validated through narrow behavioral tests rather than full-scale load work.

## Exit Criteria

- The known small high-severity defects in this workstream are closed with targeted tests.
- At least the join admission and tier-observer fixes are validated against the real runtime path.

## Open Questions

- Should the per-peer concurrency cap be scoped narrowly to inbound QUIC streams first, or should it wait for a more general admission-control framework in Workstream F?
- Is a warning in example config sufficient for deterministic dev secrets, or should example generation be changed more aggressively?
- Are there other "small but severe" defects from the assessment that should be moved into this workstream now rather than later?
