# Plan 027: Backup And Disaster Recovery Baseline

## Scope

Workstream E.6 defines the first credible backup and restore path for a
Wendelon cluster image. The baseline is intentionally narrow:

- capture the durable Control Ring artifacts and the local L1 slab set
- restore into a fresh target directory
- prove a restored node can boot and serve the recovered state

It does not yet attempt backup scheduling, remote-tier replication policy, or
cross-version restore compatibility.

## Backup Contents

The backup unit is explicit and finite:

1. Control Ring durability set
   - `control.wal`
   - `vote.wnd`
   - `snapshot.wnd` when present
2. Data Ring local slab set
   - every authoritative local `slab_<id>.wnd` file
   - active slab plus sealed slabs
3. Backup manifest
   - format version
   - last applied Control Ring position and membership summary
   - local slab ids and sizes
   - tiered-slab pointers recorded as assumptions, not inlined payloads

Not included in the baseline:

- rehydrated L1 cache copies for L2-backed slabs
- remote L2 blob bodies
- process-local caches or metrics

## Restore Target

Restore targets a fresh node data directory, not an in-place merge.

The recovered node is expected to come up with the same logical identity and
signing material as the backed-up node. This keeps the baseline honest: restore
recreates a coherent cluster image rather than inventing new membership.

## Coherence Rules

Restore writes both halves of the durable image from the same backup:

- Control Ring artifacts restore committed metadata and membership
- local slabs restore the local Data Ring bytes referenced by that metadata

This avoids the failure mode where operators recover only the Control Ring log
or only the slab files and end up with a partial node image.

Tiered slabs remain an explicit external assumption in the baseline:

- the Control Ring state keeps the authoritative L2 pointers
- backup manifests record those pointers
- restore does not inline remote-tier payloads into the backup image

## Thrash-Free Operational Semantics

The backup path uses already durable boundaries:

- the Control Ring artifacts are copied from the on-disk durability set
- the slab backup path forces `fsync` across local slabs before reading files

This makes the recovery contract testable without introducing a second commit
protocol for backup itself.

## Test Matrix

The baseline must prove:

1. backup generation succeeds against a live node image
2. restore into a fresh directory succeeds
3. a node booted from the restored image recovers Control Ring state
4. the restored node can serve previously written local slab data and accept new work

## Follow-On Work

- operator-triggered snapshot compaction before backup export
- remote-tier blob capture or remote-tier manifest replay validation
- cluster-wide restore orchestration for replacing multiple peers
