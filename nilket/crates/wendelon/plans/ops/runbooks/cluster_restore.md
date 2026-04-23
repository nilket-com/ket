# Cluster Restore

Date: 2026-04-23
Owner: `H6`
Depends On: `H1` `H2`

## When To Use This

Use this runbook when recovering a node from a `ClusterBackupImage` into a
fresh target directory or host.

Typical triggers:

- whole-node loss
- restore rehearsal
- replacement warm-start from the latest known-good storage backup

## First Look

Before restore:

- verify the correct backup image and intended target node configuration
- verify the target data directory is fresh
- confirm whether remote-tier storage is expected to be reachable during
  restore validation

After bootstrap:

- watch `/ready`
- note `degraded_reason`
- if present, note whether it is `RestoreValidationPending` or
  `RestoreValidationFailed: ...`

## Supported Contract

The supported restore path is:

1. validate the cluster backup bundle
2. restore Control Ring and slab artifacts into the target directory
3. bootstrap in recovery mode
4. reconcile tier pointers into local state
5. validate remote-tier references before the node is considered ready

Bootstrap success is not the same thing as service readiness.

## Procedure

1. Obtain the intended `ClusterBackupImage`.
2. Prepare a fresh target directory and the target bootstrap config.
3. Restore through the supported helper or the equivalent site-local workflow.
4. Wait for bootstrap completion.
5. Poll `/ready` until one of the following happens:
   - `200 OK` with `degraded_reason = null`
   - degraded with `RestoreValidationPending`
   - degraded with `RestoreValidationFailed: ...`
6. If validation is pending, keep waiting. Do not route traffic yet.
7. If validation fails, stop and investigate the referenced remote-tier
   objects before declaring the restore complete.

## Required Success Criteria

Restore is complete only when all of the following are true:

- node booted in recovery mode
- local reads against restored slab data succeed
- `/ready` returns `200 OK`
- `degraded_reason` is `null`
- if tiered references existed, remote-tier validation completed successfully

## Diagnostic Decision Tree

If `/ready` shows `RestoreValidationPending`:

- confirm remote-tier storage is reachable
- wait for validation to complete
- do not serve traffic yet

If `/ready` shows `RestoreValidationFailed: ...`:

- treat the restore as incomplete
- inspect the referenced L2/object-store path and credentials
- verify the object exists and has non-zero size
- only retry serve readiness after the tier-reference failure is resolved

If bootstrap succeeds but local reads fail:

- stop and re-check that the correct backup artifact was restored
- verify the target directory was fresh before restore

Important boundary:

- this runbook describes H2 restore into recovery mode
- the H3 replacement warm-start path does not surface
  `RestoreValidationPending` at bootstrap because it joins as a fresh node and
  receives live tier pointers later through Raft

## Reversible Mitigations First

- fix remote-tier reachability before attempting another restore
- re-run validation with the same restored artifacts if the only fault was
  external object-store reachability
- keep the node out of service until readiness is clean

Do not:

- bypass readiness and put the node into service while validation is pending
- hand-edit backup-manifest fields
- assume remote-tier payload existence just because bootstrap recovered local
  control state

## References

- `plans/impl/workstream_H_restore_runbook.md`
- `plans/impl/workstream_H_backup_manifest_spec.md`
