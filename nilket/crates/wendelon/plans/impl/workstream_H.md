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

Current status:

- artifact model implemented in `src/backup.rs`
- current canonical note: `plans/impl/workstream_H_backup_manifest_spec.md`
- single-node convenience export seam wired through `WendelonNode::export_cluster_backup()`

### H2. Restore Choreography

Implement the supported restore sequence:

- restore control state
- reattach or revalidate storage state
- validate remote-tier references
- block ready-state until validation passes

Current status:

- restore helper implemented via `WendelonNode::restore_from_cluster_backup(...)`
- readiness now stays degraded during restore validation and on tier-reference validation failure
- current canonical note: `plans/impl/workstream_H_restore_runbook.md`

### H3. Node-Replacement Contract

Define the supported replacement flow:

- detect failed or removed member
- provision or attach replacement node
- bootstrap identity
- join cluster
- verify catch-up and restored durability posture

Current status:

- node-replacement runbook written around explicit remove-and-replace semantics
- leader-side `remove_member_for_replacement(...)` helper implemented for
  dropping the failed member from Raft membership and the visible epoch before
  replacement
- replacement helper implemented via
  `restore_replacement_from_cluster_backup(...)` for seeding a fresh replacement
  identity from the failed node's storage backup
- multinode replacement proof added for remove-member, replacement join, and
  post-replacement catch-up
- current canonical note: `plans/impl/workstream_H_node_replacement_runbook.md`

### H4. Fallback Automation Path

For the earliest serious pilot, ship a tightly specified external automation contract if the full first-party operator is not ready yet.

That contract must still define:

- trigger source
- timing expectations
- required inputs
- success verification

Current status:

- executable fallback harness added at
  `scripts/recovery/run_node_replacement_contract.sh`
- canonical note written at
  `plans/impl/workstream_H_fallback_automation_contract.md`
- contract now freezes trigger source, required inputs, timeout budget, and
  success verification for external replacement automation

### H5. First-Party Operator Direction

Preserve the path to a first-party operator for the regulated or larger-scale profile.

This slice should specify:

- storage-tier-only scope
- replacement ownership
- restore/drill integration points

Current status:

- first-party operator direction note written at
  `plans/impl/workstream_H_first_party_operator_direction.md`
- scope is now frozen as storage-tier-only orchestration around the existing
  H2/H3/H4 contracts, not a generic platform controller
- replacement ownership and drill integration points are explicitly assigned so
  later operator work must compose with existing Wendelon authority surfaces

### H6. Runbooks And Drills

Write and rehearse runbooks for:

- node replacement
- pending-finalization backlog distress
- audit sink/archive distress
- cluster restore

The drill standard is non-author execution, not author reassurance.

Current status:

- operator-facing runbook catalog created at `plans/ops/runbooks/`
- canonical drill artifact created at
  `plans/impl/workstream_H_drill_report.md`
- the code/doc side of H6 is now present, but the non-author drill gate is
  still pending real execution evidence

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
- runbook catalog
- drill report
- operator or fallback automation contract note

## Deferred Follow-Ons

- point-in-time recovery
- async cross-region replication
- warm standby
- richer first-party operator automation once the base recovery contract is stable
