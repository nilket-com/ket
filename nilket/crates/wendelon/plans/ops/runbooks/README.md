# Runbook Catalog

Date: 2026-04-23
Owner: `H6`

## Purpose

This directory is the operator-facing runbook catalog for recovery and paging
conditions that Wendelon now treats as live operational contracts.

Current runbooks:

- [Node Replacement](./node_replacement.md)
  - remove-and-replace flow for a failed member using the supported H3/H4
    contract
- [Cluster Restore](./cluster_restore.md)
  - restore from a cluster backup image and validate remote-tier references
    before serving traffic
- [Pending-Finalization Backlog Distress](./pending_finalization_backlog_distress.md)
  - triage and mitigation for persisted pending-finalization buildup
- [Audit Sink And Archive Distress](./audit_sink_archive_distress.md)
  - triage for local-chain failure, archive backlog, and audit
    materialization lag
- [ADR 043 Storage Body Stabilization](./adr043_storage_body_stabilization.md)
  - post-rollout watch for Arrow IPC body compression density, CPU, L2
    admitted record-body bytes, and convergence
- [ADR 046 StateDigest Substrate](./state_digest_substrate.md)
  - substrate-only orientation, benchmark interpretation, and guards for
    absent storage/wire surfaces
- [ADR 051 Descriptor Fingerprint Split](./descriptor_fingerprint_split.md)
  - logical-vs-physical fingerprint orientation, CSB fall-through metrics,
    and rollout triage
- [ADR 052 Secondary Indexes](./secondary_indexes.md)
  - secondary-index lifecycle, sidecar coverage, fail-closed fallback, and
    drop-GC triage
- [ADR 053 Wire Arrow IPC Body Compression](./wire_arrow_ipc_body_compression.md)
  - transport-side Arrow IPC LZ4 rollout, metrics, decode-error triage,
    and frame-flag rejection interpretation
- [ADR 054 CDC Subscription Substrate](./cdc_subscription.md)
  - CDC cursor/reject interpretation, live-tail ring overflow, heartbeat
    stalls, and bounded `cdc_*` metric surface
- [ADR 055 LSM Compaction](./lsm_compaction.md)
  - L0 debt, compaction result labels, retired pin pressure, digest invariant
    alarms, and bounded `lsm_*` metric surface
- [ADR 056 Row-Group Fetch](./row_group_fetch.md)
  - row-group strategy mix, fallback reasons, savings metrics, tuning knobs,
    and emergency disable procedure

These runbooks complement, but do not replace, the deeper implementation notes
in the durable H support contracts under `plans/ops/runbooks/` and `plans/ops/`.
