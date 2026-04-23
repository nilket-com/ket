# Audit Sink And Archive Distress

Date: 2026-04-23
Owner: `H6`
Depends On: `G`

## When To Use This

Use this runbook when audit durability or audit retention signals are in
distress.

Primary triggers:

- `AuditSinkFailing`
- `AuditMaterializationLagRising`
- sustained archive backlog or archive queue drops

## First Look

Check:

- `/ready`
  - `audit_materialization_lag_entries`
  - `degraded_reason`
- `wendelon_audit_sink_failure_total{sink="local",reason="emit_error"}`
- `wendelon_audit_sink_failure_total{sink="archive",reason="emit_error"}`
- `wendelon_audit_sink_failure_total{sink="archive",reason="queue_drop"}`
- `wendelon_audit_archive_pending_records`
- `wendelon_audit_archived_segments_total`

Interpretation:

- local sink failure is the serious path
- archive distress is still important, but local authority remains the baseline
- non-zero audit materialization lag degrades readiness with
  `degraded_reason = "AuditLagExceeded"`

## Why This Pages

The local audit chain is authoritative for serious deployments.

Operational implications:

- local audit failure can reject node-local security and data-access operations
- archive failure does not replace the local chain, but it can break the
  long-horizon evidence path
- audit materialization lag means the local chain is not keeping up with
  raft-authoritative events and readiness should stay degraded

## Decision Tree

If local sink failures are increasing:

- treat as a page
- confirm the local audit path is writable
- inspect disk, permissions, and filesystem health on the audit directory
- keep the node out of service if audit failures are still live

If readiness is degraded with `AuditLagExceeded`:

- confirm `wendelon_audit_materialization_lag_entries` is actually falling
- if lag is rising, inspect local disk saturation and recent audit sink
  failures

If archive failures are increasing but local sink is healthy:

- local authority still exists
- investigate archive path reachability and free space
- archive backlog is in-memory only today, so prolonged archive outage still
  needs operator action

If archive queue drops are non-zero:

- the archive path was unavailable long enough to overflow the in-memory
  backlog
- plan a local-chain reconciliation/export after recovery if archive retention
  evidence matters for the incident window

## Mitigation Actions

Ranked by reversibility:

1. restore local filesystem writability for the audit baseline
2. restore archive-path reachability or free space
3. keep the node out of service until `/ready` is clean again if local audit
   failure or lag persists
4. reconcile archive retention from the local NDJSON chain after archive-path
   recovery if queue drops occurred

Do not:

- treat Kafka or Syslog success as proof that the authoritative local chain is
  healthy
- clear the incident just because archive failures stopped while local lag is
  still rising
- delete `audit.ndjson` because the archive mirror exists

## Success Criteria

- `wendelon_audit_sink_failure_total{sink="local",reason="emit_error"}` stops
  increasing
- `audit_materialization_lag_entries` returns to `0`
- `/ready` clears `AuditLagExceeded`
- if archive was affected, `wendelon_audit_archive_pending_records` returns to
  baseline and archive queue drops stop

## Escalate When

Escalate if:

- local sink failures continue for more than one paging interval
- readiness remains degraded by audit lag after local sink recovery
- archive queue drops are non-zero for an evidence-retention deployment
- the incident affects more than one node at once

## References

- `plans/ops/audit.md`
- `plans/impl/workstream_G_evidence_path_note.md`
- `plans/impl/workstream_G_archive_retention_guide.md`
