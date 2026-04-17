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

## Deferred

- scheduled export and retention management
- multi-node cluster replacement choreography
- remote-tier payload validation during restore
- legal-hold specific backup policy
