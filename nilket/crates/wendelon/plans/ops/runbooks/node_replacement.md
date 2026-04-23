# Node Replacement

Date: 2026-04-23
Owner: `H6`
Depends On: `H3` `H4`

## When To Use This

Use this runbook when a node host, local data directory, or local persistence
substrate is no longer trustworthy and the supported answer is replacement, not
in-place repair.

Typical triggers:

- repeated host or disk faults
- fenced node with `degraded_reason = "LocalPersistenceDistrust"`
- permanent host loss
- failed node removed from service by operator decision

## First Look

Confirm the replacement is actually warranted before changing membership.

Check:

- `/ready`
  - note `degraded_reason`
- cluster membership and current leader
- whether the failed node is still reachable enough to export or retrieve the
  latest `ClusterBackupImage`

If the node is merely lagging but otherwise healthy, do not use this runbook.
This flow is for replace-or-reseed, not for transient performance variance.

## Supported Contract

The supported day-one flow is:

1. remove the failed member identity from the live cluster
2. provision a fresh replacement node identity
3. optionally seed it from the failed node's latest storage backup
4. start it with `cluster.mode = "join"`
5. verify membership, readiness, and catch-up before declaring success

Do not attempt same-identity hot replacement in place. The supported H3
contract is explicit remove-and-replace with a fresh node identity.

This also means replacement warm-start does not use the H2 recovery readiness
gate. The replacement joins as a fresh node and receives live state through the
cluster; operators should not expect `RestoreValidationPending` on replacement
bootstrap.

## Procedure

1. Capture the failure context.
   - record the failed node id
   - capture current leader and current membership
   - preserve relevant logs before host teardown if they are still available
2. Remove the failed member from live membership.
   - run the removal against the current leader
   - use the supported leader-side removal flow
   - if using the fallback harness, this is the `--remove-member-cmd` step
3. Provision a fresh target.
   - fresh host or fresh data directory
   - fresh `node_id`
   - fresh advertised transport address
   - fresh identity material for the replacement node
4. Decide whether to warm-start from backup.
   - if yes, retrieve the latest `ClusterBackupImage`
   - pass the failed node id as the backup source and the replacement config as
     the target
5. Start the replacement in join mode.
   - if using the fallback harness, this is `--provision-replacement-cmd` and
     `--start-replacement-cmd`
6. Verify the replacement.
   - membership shows the failed member absent
   - membership shows the replacement member present
   - replacement reports `/ready`
   - replacement sees the current leader
   - writes committed after the backup cut have replayed to the replacement

## Verification Standard

Replacement is complete only when all of the following are true:

- old member identity removed
- replacement member present in the expected voter set
- replacement `/ready` returns `200 OK`
- replacement `degraded_reason` is `null`
- fresh committed state, not just the backup image, is visible on the
  replacement

The fallback harness already freezes these checks as:

- `membership`
- `ready`
- `catch_up`

## Reversible Mitigations First

Prefer these before destructive follow-up:

- stop routing client traffic to the failed node
- preserve the latest good backup image before reimaging hardware
- stabilize surviving seeds and leadership before introducing the replacement

Do not:

- manually edit Raft artifacts on disk
- reuse the failed node id for the replacement
- declare success once the replacement merely boots

## Escalate When

Escalate immediately if:

- the failed node cannot be removed cleanly from membership
- the replacement joins but never reports ready
- the replacement reports ready but does not catch up with post-backup commits
- replacement is blocked by missing or invalid backup material

## References

- `scripts/recovery/run_node_replacement_contract.sh`
- `plans/impl/workstream_H_node_replacement_runbook.md`
- `plans/impl/workstream_H_fallback_automation_contract.md`
