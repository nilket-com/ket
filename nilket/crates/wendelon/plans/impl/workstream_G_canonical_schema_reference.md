# Workstream G Canonical Audit Schema Reference

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `G1`

## Purpose

Freeze the minimum typed audit schema that serious downstream consumers can rely on.

Free-form messages remain allowed for operator readability, but they are no longer the only machine-readable surface for the minimum serious families.

## Canonical Families

`AuditEvent` and `PersistedAuditRecord` now carry an optional `schema` object in addition to:

- `authority`
- `operation`
- `event_type`
- `tenant_id`
- `record_identity`
- `actor`
- `outcome`
- `raft_log_index`
- `message`
- `parameters`

The `schema` object is a tagged union:

- `administrative_control`
- `local_security`
- `data_access`

## Administrative Control

Type:

- `schema.type = "administrative_control"`

Fields:

- `action`
- `target_tenant_id`
- `target_tenant_kek_id`
- `target_role`
- `target_scope`
- `target_node_id`
- `from_epoch`
- `to_epoch`
- `target_trust_domain`
- `target_key_id`

Current live actions:

- `advance_epoch`
- `tenant_create`
- `tenant_quota_update`
- `tenant_suspend`
- `tenant_resume`
- `tenant_delete_request`
- `tenant_kek_rotate`
- `tenant_kek_retire`
- `actor_key_revoke`
- `break_glass_issue`

## Local Security

Type:

- `schema.type = "local_security"`

Fields:

- `surface`
- `stage`
- `path`
- `required_permission`
- `failure_reason`

Current live producer:

- observability mTLS rejection / authorization failure

Current live stages:

- `tls_handshake`
- `peer_identity`
- `authorization`

## Data Access

Type:

- `schema.type = "data_access"`

Fields:

- `access_kind`
- `phase`
- `locality`
- `locality_end`
- `durability`
- `charged_bytes`
- `returned_bytes`
- `result_count`

Current live producer:

- dispatcher-emitted `data.write`
- dispatcher-emitted `data.read`
- dispatcher-emitted `data.range_query`
- Raft-authoritative `data.write_finalized`

Current live phases:

- `requested`
- `completed`
- `denied`
- `finalized`

Current interpretation:

- `denied`
  - refusal before execution
- `completed`
  - execution reached a terminal response; `outcome = "error"` may still appear here for non-OK completion statuses surfaced after dispatch
- `finalized`
  - Raft-authoritative durable finalization evidence for writes

This is still intentionally the first floor, not the final ceiling. Later `G` slices may add richer typed detail without changing the union family boundary.

## Compatibility

- local NDJSON remains the authoritative baseline
- older local audit records without `schema` continue to deserialize
- the local chain format is now at schema version `4`
- off-box sinks still ship projections, not the authoritative local chain

## Evidence

- `src/audit.rs::tests::audit_event_serializes_typed_schema_detail`
- `tests/observability_listeners.rs::observability_rejects_wrong_ca_domains_and_marks_break_glass_audit`
- `tests/control_ring_rpc.rs::advance_epoch_commit_emits_raft_audit_once_across_restart`
- `src/consensus/raft_storage.rs` Raft-authoritative mutation audit materialization
