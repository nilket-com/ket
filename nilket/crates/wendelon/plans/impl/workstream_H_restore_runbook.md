# Workstream H Restore Runbook

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `H2`

## Purpose

Freeze the first supported restore sequence for Wendelon so restore stops being "copy a few files and hope bootstrap means healthy".

The current implementation now has a supported library entrypoint:

- `WendelonNode::restore_from_cluster_backup(&ClusterBackupImage, NodeBootstrapConfig)`

## Current Supported Sequence

The supported restore choreography is:

1. Validate the backup bundle integrity.
2. Select the embedded node-storage artifact for the requested `node_id`.
3. Restore Control Ring artifacts into the target data directory.
4. Restore local slab artifacts into the same target directory.
5. Bootstrap the node in ordinary recovery mode from those restored artifacts.
6. Reconcile any replicated tier pointers from recovered Control Ring state into the local slab manager.
7. Validate remote-tier references before the node advertises ready.

## Readiness Gate

Restore validation is now an explicit readiness gate.

Behavior:

- if recovery mode has no replicated tier references
  - readiness is unaffected by restore validation
- if recovery mode has replicated tier references
  - readiness starts degraded with `RestoreValidationPending`
  - a background restore-validation pass checks every referenced L2 object via `head`
  - readiness clears only after that pass succeeds
- if validation fails
  - readiness stays degraded with `RestoreValidationFailed: ...`

This is intentionally narrower than a full traffic fence:

- the current `H2` contract blocks `/ready`
- it does not yet add a deeper admission fence beyond the existing readiness posture

That is acceptable for this phase because the serious deployment contract already assumes load-balancers and operators use readiness as the serve/no-serve gate.

## Remote-Tier Validation Scope

Current validation proves:

- an L2 store is configured when replicated tier pointers exist
- every referenced tier object is reachable by URI
- every referenced object reports non-zero size

Current validation does not yet:

- re-read and hash every remote tier object during restore
- prefetch or rehydrate tiered slabs into local storage
- cross-check archive / backup storage against live L2 independently

Those are possible later hardening layers, but they are not required for the first restore choreography slice.

## Operator Sequence

Current operator/library sequence for a single-node restore:

1. obtain `ClusterBackupImage`
2. choose the target node configuration
3. call `WendelonNode::restore_from_cluster_backup(...)`
4. wait for bootstrap to complete
5. check `/ready`
6. only serve traffic after readiness returns `200 OK`

If readiness remains degraded with `RestoreValidationFailed: ...`, the restore is not complete even though durable recovery bootstrap succeeded.

Important boundary:

- this readiness gate applies to the H2 restore path that reinstalls Control
  Ring artifacts and boots in recovery mode
- the H3 replacement warm-start path does not begin in recovery mode because it
  restores slab data only and receives live tier pointers later through normal
  Raft replication
- operators should not assume a replacement warm-start will surface
  `RestoreValidationPending`

## Evidence

- `tests/backup_restore.rs::whole_node_loss_restore_bootstraps_in_recovery_mode_and_serves_recovered_state`
- `src/node.rs::tests::single_node_bootstrap_reconciles_tiered_slabs_from_recovered_state`
- `src/node.rs::tests::recovery_blocks_readiness_when_tiered_reference_validation_fails`
