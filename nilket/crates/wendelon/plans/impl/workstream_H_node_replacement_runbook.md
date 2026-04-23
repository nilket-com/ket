# Workstream H Node-Replacement Runbook

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `H3`

## Purpose

Freeze the first supported node-replacement contract so "replace the failed box"
stops meaning "try some bootstrap flags and hope the cluster likes it".

## Current Supported Contract

The current supported replacement path is:

- remove the failed member identity from the live cluster first
- provision a fresh replacement node identity
- optionally seed that replacement from the failed node's latest storage backup
- start the replacement with ordinary `cluster.mode = "join"` configuration
  pointing at live seeds

This slice does **not** yet support:

- automatic cloud or machine provisioning
- automatic failed-member detection inside the database binary

Those remain later automation concerns. The day-one contract is an explicit,
rehearsable remove-and-replace flow.

Important boundary:

- replacement warm-start restores slab data from backup
- it does not restore Control Ring artifacts for the replacement identity
- the replacement therefore boots as a fresh join node, not as an H2 recovery
  node
- `RestoreValidationPending` is part of the H2 restore contract, not the H3
  replacement-join contract

## Supported Sequence

1. Detect that a node host or local disk is no longer trustworthy.
2. On the current leader, remove that failed member identity from the cluster's current voter set and
   visible epoch.
3. Provision a fresh host or fresh target directory for the replacement node.
4. Bootstrap a fresh replacement identity: new `node_id`, new advertised
   address, and corresponding identity material.
5. Obtain the latest `ClusterBackupImage` for the failed node if local storage
   warm-start is desired.
6. Call `WendelonNode::restore_replacement_from_cluster_backup(...)` with a
   replacement bootstrap config whose cluster mode is `Join`.
7. Wait for the replacement node to:
   - boot as the replacement node identity
   - rejoin the live cluster
   - report `/ready`
8. Verify that post-backup committed state has caught up on the replacement
   before declaring the replacement complete.

## Verification Standard

The replacement is complete only when all of the following are true:

- the replacement node reports ready
- the live cluster membership shows the old member removed and the replacement
  member present in the expected voter set
- the replacement node sees the current leader
- state committed after the backup cut has replayed onto the replacement node

That last point matters. A restored replacement is not complete just because it
can read the backup image; it must catch up to the surviving cluster's current
authoritative state.

## Current API Surface

The supported library entrypoints are:

- `WendelonNode::remove_member_for_replacement(node_id)`
- `WendelonNode::restore_replacement_from_cluster_backup(&ClusterBackupImage, source_node_id, NodeBootstrapConfig)`

`remove_member_for_replacement(...)` is intentionally leader-side. Call it on
the current leader for deterministic behavior.

The important H3 rule is not a different artifact format. It is the supported
operational contract around the same helper:

- the failed member identity is removed first
- the replacement target is a fresh host or fresh data directory
- the replacement uses a fresh node identity
- the replacement may reuse the failed node's latest-good storage backup
- the replacement starts with `cluster.mode = "join"` so it can reattach to the
  surviving cluster

After the replacement is back, verification must prove new committed state
replicates again. The replacement is not complete just because the backup image
boots.

## Evidence

- `tests/multinode_bootstrap.rs::restored_replacement_node_rejoins_live_cluster_and_catches_up`
- `tests/multinode_bootstrap.rs::explicit_join_is_idempotent_and_recovery_rejoins_cleanly`
- `tests/backup_restore.rs::whole_node_loss_restore_bootstraps_in_recovery_mode_and_serves_recovered_state`
