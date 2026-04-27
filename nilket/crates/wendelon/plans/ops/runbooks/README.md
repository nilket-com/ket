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

These runbooks complement, but do not replace, the deeper implementation notes
in the durable H support contracts under `plans/ops/runbooks/` and `plans/ops/`.
