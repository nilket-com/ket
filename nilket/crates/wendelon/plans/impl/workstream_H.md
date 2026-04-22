# Workstream H: Node Replacement, DR, And Operator Runbooks

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS7`

## Objective

Close the gap between mathematical healing and real operational recovery so the system can be restored and repaired without tribal knowledge.

## Why Now

ADR `001` was right to stop at eviction. It was never a full recovery story.

This workstream is where Wendelon proves that:

- backups are real
- restores are real
- node replacement is real

and not just assumptions hidden in a founder's head.

## ADR Anchors

- ADR `001`
- ADR `033`
- ADR `025`
- ADR `030`

## Scope

- cluster-scoped backup contract
- restore choreography and validation
- node-replacement contract
- fallback automation path
- first-party operator direction
- backup encryption integration
- runbooks and drills
- explicit day-one versus day-two recovery posture

## Out Of Scope

- low-level write-path correctness
- Toron compute semantics
- final regulated compliance packaging

## Dependencies

- `B` for correct write/recovery semantics
- `E` for backup encryption and key handling
- `F` for operator authorization
- `G` for auditable recovery and operator actions

## Entry Criteria

- ADR `033` is the authority for backup/restore/replacement semantics
- the authoritative custody path for backup encryption is selected
- the minimum operator identity model exists

## Implementation Slices

### H1. Backup Manifest And Consistent Cut

Define and implement the backup artifact model:

- control-state point
- storage metadata
- data references or data copies required for restore
- manifest integrity fields

Per-node anecdotal snapshots are not sufficient.

### H2. Restore Choreography

Implement the supported restore sequence:

- restore control state
- reattach or revalidate storage state
- validate remote-tier references
- block ready-state until validation passes

### H3. Node-Replacement Contract

Define the supported replacement flow:

- detect failed or removed member
- provision or attach replacement node
- bootstrap identity
- join cluster
- verify catch-up and restored durability posture

### H4. Fallback Automation Path

For the earliest serious pilot, ship a tightly specified external automation contract if the full first-party operator is not ready yet.

That contract must still define:

- trigger source
- timing expectations
- required inputs
- success verification

### H5. First-Party Operator Direction

Preserve the path to a first-party operator for the regulated or larger-scale profile.

This slice should specify:

- storage-tier-only scope
- replacement ownership
- restore/drill integration points

### H6. Runbooks And Drills

Write and rehearse runbooks for:

- node replacement
- pending-finalization backlog distress
- audit sink/archive distress
- cluster restore

The drill standard is non-author execution, not author reassurance.

## Tests And Benchmarks

- backup-manifest integrity tests
- restore integration tests
- remote-tier validation tests
- replacement-under-load tests
- backup-encryption and restore tests
- runbook-only drill rehearsals
- timed replacement and restore reports

## Exit Criteria

- backup artifacts are cluster-scoped and documented
- restore blocks readiness until validation is complete
- a supported node-replacement path exists
- at least one non-author restore or replacement drill has succeeded
- the runbooks are good enough to be operational artifacts rather than internal notes

## Artifacts

- backup manifest spec
- restore runbook
- node-replacement runbook
- drill report
- operator or fallback automation contract note

## Deferred Follow-Ons

- point-in-time recovery
- async cross-region replication
- warm standby
- richer first-party operator automation once the base recovery contract is stable
