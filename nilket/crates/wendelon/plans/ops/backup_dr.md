# Backup And DR

## Baseline Unit

The baseline backup unit is a full node image rooted at one data directory:

- Control Ring artifacts: `control.wal`, `vote.wnd`, `snapshot.wnd` when present
- local Data Ring slab files: active and sealed `slab_<id>.wnd`
- a manifest that records the recovered Control Ring position, local slab ids,
  and any tiered-slab assumptions

This is the minimum credible unit because backing up only the Control Ring log
or only the slab files yields a partial image.

## Restore Target

Restore targets a fresh directory for a replacement node that keeps the same
logical identity and signing material as the backed-up node.

The baseline restore flow is:

1. provision an empty target directory
2. write the backed-up Control Ring artifacts into that directory
3. restore the backed-up local slab files
4. start the node with the normal bootstrap path

## Replacement

For a live cluster, the supported path is:

1. remove the failed member identity from the live cluster
2. provision a fresh replacement host or directory
3. assign the replacement a fresh node identity
4. optionally restore the failed node's latest storage backup into that target
5. start the replacement with `cluster.mode = "join"` pointing at surviving
   seeds
6. wait for `/ready`
7. verify the replacement has caught up with state committed after the backup
   cut before declaring the replacement complete

For the early pilot profile, the external automation contract is the executable
harness at `scripts/recovery/run_node_replacement_contract.sh`. It does not
perform provisioning or admin RPCs by itself; it enforces the required step
ordering, deadlines, and verification hooks around whatever site-local tooling
executes those actions.

For the later larger-scale or regulated profile, the intended Wendelon-owned
operator remains storage-tier-only. It may absorb the replacement / restore
workflow orchestration above, but it does not expand into general application
or identity-platform ownership.

## Remote-Tier Assumptions

Tiered slabs are treated as external durability in the baseline:

- the authoritative pointer lives in the Control Ring state
- the backup manifest records those pointers explicitly
- the backup does not inline remote-tier payload bodies

Operators therefore still need durable object-store policy outside Wendelon
for tiered data, including cross-region replication if the deployment requires
it.

## Recovery Checks

After restore, operators should verify:

- the node boots in recovery mode rather than fresh bootstrap mode
- the recovered Control Ring membership and epoch history are present
- local reads against backed-up slab data succeed
- new writes and new Control Ring commits also succeed

## Runbooks

The current operator-facing recovery catalog is:

- `plans/ops/runbooks/cluster_restore.md`
- `plans/ops/runbooks/node_replacement.md`

The implementation details are code-backed by the backup/restore and multinode
replacement tests; the runbooks above are the operator-facing procedures.

## Deferred

- scheduled export and retention management
- remote-tier payload validation during restore
- automated failed-member detection and external replacement triggering
- legal-hold specific backup policy
