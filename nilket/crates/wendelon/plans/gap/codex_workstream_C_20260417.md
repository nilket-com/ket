# Codex Workstream C: Security Floor And Encryption At Rest

Date: 2026-04-17

Derived from:
- `plans/gap/codex_execution_20260417.md`

## Objective

Establish the minimum security posture required for a serious production pilot.

This workstream is intentionally focused on the security floor, not the entire long-term policy model. The aim is to remove pilot-blocking security gaps: at-rest encryption absence, lack of baseline authorization separation, missing auditability, and incomplete key/cert lifecycle direction.

## In Scope

- Short ADR/design pass for:
  - encryption at rest
  - control-plane authorization model
  - key/cert rotation expectations
- Implement encryption at rest for:
  - L1 slabs
  - WAL
  - snapshots
  - L2 objects
- Baseline control-plane authorization boundaries.
  - preserve distinction between operator authority and data-node authority
- Audit stream for privileged or operationally sensitive actions.
- Initial key/cert lifecycle support.
  - certificate reload/rotation direction
  - signing-key rotation direction

## Out Of Scope For This Pass

- Full multi-tenant RBAC and namespace model.
- Mature compliance program artifacts beyond the product surface itself.
- Performance optimization of crypto paths beyond what is needed to land the floor safely.

## Primary Deliverables

- Persisted data is encrypted at rest across the core storage surfaces.
- Control-plane authority is no longer flat among all trusted peers.
- Privileged actions produce auditable records.
- Rotation direction exists for certificates and signing keys.

## Key Interfaces And Dependencies

- Depends on Workstream A for real-path runtime validation.
- Benefits from Workstream B landing join admission fixes early.
- Must coordinate with:
  - Workstream D on snapshot/WAL artifact handling
  - Workstream E on L2 object behavior
  - Workstream G on secure metrics and operational control surfaces
  - Workstream H for eventual tenant-level isolation

## First-Pass Sequencing

1. Write the short ADRs/design notes.
2. Define storage-format and key-management boundaries clearly enough to parallelize implementation.
3. Implement at-rest crypto on local persisted artifacts.
4. Implement L2 at-rest crypto behavior.
5. Add baseline authorization separation for control-plane actions.
6. Add audit stream.
7. Add initial rotation support and verification coverage.

## Validation Shape

- Restart tests proving encrypted artifacts remain readable with correct key material.
- Negative tests for unauthorized control-plane actions.
- Audit-log presence tests for privileged actions.
- Key/cert lifecycle tests at minimum for reload/rotation baseline flows.

## Exit Criteria

- Sensitive persisted data is encrypted at rest.
- Privileged control actions are authorization-aware.
- Privileged actions are auditable.
- Key/cert lifecycle direction is implemented at least to a baseline operational level.

## Open Questions

- Should the first implementation use client-side envelope encryption for L2, provider-managed SSE-KMS, or support both from the start?
- How narrow can the initial control-plane authorization model be while still being credible for a pilot?
- Should audit output start as local append-only files, or should a structured event stream abstraction be introduced immediately?
