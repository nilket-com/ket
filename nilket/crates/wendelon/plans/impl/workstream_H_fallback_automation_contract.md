# Workstream H Fallback Automation Contract

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `H4`

## Purpose

Freeze the minimum acceptable external automation contract for the first serious
pilot while the full first-party operator is still deferred.

The goal is not "full automation". The goal is to remove hidden author
knowledge from the replacement path.

## Current Contract

The canonical H4 contract is the executable harness:

- `scripts/recovery/run_node_replacement_contract.sh`

It is intentionally a hook-based harness, not a Wendelon-native controller:

- Wendelon still owns backup, restore, membership removal, and readiness
  semantics
- external automation still owns machine provisioning, service startup, and how
  it invokes admin actions against the live cluster

## Required Trigger Sources

Accepted trigger categories for this contract are:

- `manual_declared_failure`
- `scheduled_drill`
- `hardware_loss`
- `local_storage_loss`

The harness does not hard-code that taxonomy, but the automation invoking it
must set `--trigger-source` to one of those categories and preserve it in the
execution record.

## Required Inputs

The contract requires:

- `--trigger-source`
- `--failed-node-id`
- `--replacement-node-id`
- `--replacement-config`
- `--remove-member-cmd`
- `--provision-replacement-cmd`
- `--start-replacement-cmd`
- `--membership-check-cmd`
- `--ready-check-cmd`
- `--catch-up-check-cmd`

Optional but expected in the serious pilot profile:

- `--backup-source-node-id`
- `--backup-artifact`

That reflects the H3 remove-and-replace contract:

- drop the failed member identity
- bring up a fresh replacement identity
- optionally seed from the failed node's latest-good storage backup

## Timing Expectations

The harness enforces explicit deadlines for every phase:

- remove member: default `60s`
- provision replacement: default `600s`
- start replacement: default `120s`
- membership verification: default `120s`
- readiness verification: default `300s`
- catch-up verification: default `300s`

The caller may tighten or relax those with the `--*-timeout-sec` flags, but a
serious automation flow must declare them explicitly rather than letting each
step hang forever.

The current harness now enforces those phase deadlines in-process:

- one-shot command phases are wrapped with `timeout`
- verification phases stay wall-clock bounded through polling

## Success Verification

The replacement is only considered successful if all three verification hooks
eventually succeed:

- `membership-check-cmd`
  - proves the failed member is gone and the replacement member is present in
    the intended voter set
- `ready-check-cmd`
  - proves the replacement node is serving the ordinary readiness contract
- `catch-up-check-cmd`
  - proves post-replacement committed state has replicated onto the replacement

This is stricter than "the process started". That is intentional.

## Execution Record

The harness writes:

- `report.env`

into the selected output directory. That record captures:

- trigger source
- failed and replacement identities
- backup inputs
- timeout values
- start and completion timestamps

This is the minimum H4 auditability bar for external automation.

On failure, `report.env` now records:

- `status=failed`
- `failed_step=<phase>`
- `completed_at=<timestamp>`

## Example Shape

The expected invocation shape is:

```bash
scripts/recovery/run_node_replacement_contract.sh \
  --trigger-source scheduled_drill \
  --failed-node-id 3 \
  --replacement-node-id 4 \
  --replacement-config /etc/wendelon/node4.toml \
  --backup-source-node-id 3 \
  --backup-artifact /var/backups/wendelon/node3.cluster-backup.json \
  --remove-member-cmd 'ops/remove-member 3' \
  --provision-replacement-cmd 'ops/provision-node 4' \
  --start-replacement-cmd 'ops/start-node-from-backup 3 4 /etc/wendelon/node4.toml' \
  --membership-check-cmd 'ops/check-membership 1,2,4' \
  --ready-check-cmd 'ops/check-ready 4' \
  --catch-up-check-cmd 'ops/check-catch-up 4'
```

The harness does not prescribe those helper command names. It prescribes their
semantic role and ordering.

## Evidence

- `scripts/recovery/run_node_replacement_contract.sh`
