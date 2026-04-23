# Workstream G Evidence Path Note

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `G2`

## Purpose

Make the authority-versus-materialization model explicit enough that operators and code both answer the same question the same way:

- what event is authoritative where
- what sink is a required materialization
- what sink is only a projection

## Authority Model

Wendelon now carries an explicit `authority_model` alongside `authority` on audit events and persisted local-chain records.

Values:

- `node_local_authoritative`
- `raft_authoritative_materialized`

Interpretation:

- `authority = node_local`
  - the event originates on this node
  - the local NDJSON chain is the authoritative baseline for that event
- `authority = raft_log`
  - the event originates from a committed state-machine mutation
  - the committed Raft log is authoritative
  - the local NDJSON chain is the required serious-deployment materialization of that committed authority

## Sink Model

The sink layer now exposes the evidence disposition explicitly:

- `authoritative_local_chain = true` means the sink surface includes the required local NDJSON chain
- `archived_local_chain = true` means the sink surface also maintains the segmented archive mirror of that local chain
- `archive_lock_mode` records the requested archive lock posture when archive mirroring is enabled
- `kafka_canonical_chain = true` means Kafka is carrying the exact persisted local-chain record rather than the reduced event body
- `projection_sinks` lists additive off-box projections such as `syslog` or `kafka`

Current intended posture:

- `LocalAuditSink`
  - authoritative local chain
  - optionally archive-mirrored
- `CompositeAuditSink`
  - authoritative local chain plus zero or more projection sinks
  - may also indicate canonical Kafka shipping when configured
- `Kafka` and `Syslog`
  - projections
  - Kafka can optionally carry canonical local-chain records for delivered messages, but still does not replace the local baseline

For dispatcher-visible data-access events, failure to append to the local authoritative chain is fail-closed:

- `data.write`
- `data.read`
- `data.range_query`

The request is refused rather than proceeding without the required local evidence.

## Serious Deployment Rule

For a serious deployment, local NDJSON is mandatory and authoritative for local events.

Off-box sinks are optional shipping paths:

- useful for SIEM or archive workflows
- not sufficient by themselves for local chain replay
- not allowed to silently replace the local chain

## Current Live Producers

`raft_authoritative_materialized`

- committed tenant lifecycle mutations
- epoch advancement
- tenant-KEK rotate/retire
- actor-key revocation
- finalized write evidence

`node_local_authoritative`

- observability access failures
- observability success events
- dispatcher `data.write`
- dispatcher `data.read`
- dispatcher `data.range_query`
- crypto startup and decrypt rejection
- MVCC/tiering local maintenance events
- break-glass issuance

## Evidence

- `src/audit.rs::tests::audit_event_serializes_authority_model`
- `src/audit.rs::tests::local_and_composite_sinks_expose_evidence_disposition`
- `tests/observability_listeners.rs::observability_rejects_wrong_ca_domains_and_marks_break_glass_audit`
- `src/consensus/raft_storage.rs` Raft audit materializer comments and typed authority-model emission
