# Workstream H Backup Manifest Spec

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `H1`

## Purpose

Freeze the first supported backup artifact model so backup generation stops being "a Control Ring export plus whatever storage bytes happened to be nearby".

The live code now has an explicit cluster backup bundle:

- `ClusterBackupImage`
- `ClusterBackupManifest`
- `ClusterBackupConsistentCut`
- `ClusterNodeBackupImage`
- `ClusterNodeBackupManifest`

## Artifact Shape

Top-level bundle:

- `manifest`
- `control`
- `nodes[]`

The top-level manifest is the recovery contract. The embedded control and node-storage images are the payloads the manifest describes and integrity-checks.

## Consistent Cut

The manifest carries a single explicit cut:

- `last_applied_log_id`
- `current_epoch`
- `last_e_safe_index`
- `voter_ids`
- `control_state_hash`

Interpretation:

- the cut is anchored to the authoritative Control Ring state snapshot used at export time
- `control_state_hash` is the hash of that serialized state snapshot
- the embedded Control Ring backup image must agree with:
  - `last_applied_log_id`
  - `voter_ids`
  - `control_state_hash`

This is the first floor of "cluster-scoped consistent cut" for Wendelon:

- one explicit cut object
- one authoritative control image
- one or more node-storage artifacts tied to that same cut

## Per-Node Storage Manifest

Each node manifest records:

- `node_id`
- `active_slab_id`
- `next_slab_id`
- `local_copy_slab_ids`
- `local_copy_slab_bytes`
- `tiered_references`
- `omitted_rehydrated_slabs`
- `storage_image_hash`

Meaning:

- local slab bytes embedded in the node image are the copied-on-backup L1 material required for restore
- `tiered_references` are remote-tier references that a later restore must revalidate before declaring readiness
- `omitted_rehydrated_slabs` are explicitly not part of the backup data set because they are cache copies, not authoritative state

## Integrity Fields

The manifest carries integrity digests for every embedded artifact:

- `control_image_hash`
- per-node `storage_image_hash`

`ClusterBackupImage::validate_integrity()` currently verifies:

- manifest format version
- control-image hash match
- control-cut agreement with the embedded Control Ring image
- unique node ids
- node-manifest equality with the embedded storage images
- per-node storage-image hash match

## Current Export Surface

Live now:

- `WendelonNode::export_cluster_backup()`
  - convenient single-node export seam for the current restore tests
- `ClusterBackupImage::assemble(...)`
  - general assembly API for future multi-node backup orchestration

Important boundary:

- the artifact model is cluster-scoped now
- the convenience node method still exports only the local node's storage artifact
- coordinated multi-node capture and restore choreography remain later `H` slices

That split is intentional for `H1`: freeze the artifact model first, then build restore and replacement workflows on top of it.

## Evidence

- `tests/backup_restore.rs::backup_generation_exports_control_and_storage_artifacts`
- `tests/backup_restore.rs::backup_manifest_detects_tampered_embedded_storage_artifact`
- `tests/backup_restore.rs::whole_node_loss_restore_bootstraps_in_recovery_mode_and_serves_recovered_state`
