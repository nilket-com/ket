# Workstream F1: Role Matrix

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `F`

## Objective

Land the minimum serious operator-role split in code without waiting for cert-service or IdP work.

## Roles

Wendelon now recognizes these serious admin roles in the identity layer:

- `tenant_admin`
- `cluster_admin`
- `backup_admin`
- `compliance_auditor`

Existing roles remain:

- `data_node`
- `read_only_observer`
- `break_glass_admin`
- legacy `operator`

The legacy `operator` role remains as a compatibility superset during the transition. It is no longer the only serious administrative model.

## Dispatcher Matrix

| Surface | Allowed roles |
|---|---|
| Inter-node transport | `data_node` |
| Control status | `data_node`, `cluster_admin`, `backup_admin`, `compliance_auditor`, `read_only_observer`, `break_glass_admin`, legacy `operator` |
| Cluster control (`join`, `lease`, `lease_release`, `transfer`, `advance_epoch`) | `data_node`, `cluster_admin`, `break_glass_admin`, legacy `operator` |
| Audit-chain read | `compliance_auditor`, `read_only_observer`, `break_glass_admin`, legacy `operator` |
| TenantAdmin read (`get`, `list`) | `tenant_admin`, `backup_admin`, `read_only_observer`, `break_glass_admin`, legacy `operator` |
| Tenant lifecycle mutation (`create`, `update_quota`, `suspend`, `resume`, `delete`) | `tenant_admin`, `break_glass_admin`, legacy `operator` |
| Tenant KEK mutation (`rotate_tenant_kek`, `retire_tenant_kek`) | `backup_admin`, `break_glass_admin`, legacy `operator` |
| Observability `/ready` | `data_node` plus all ops-domain human roles, `read_only_observer`, `break_glass_admin` |
| Observability `/metrics` | all ops-domain human roles, `read_only_observer`, `break_glass_admin` |

## Implementation Notes

- The split is enforced at dispatcher/authorization boundaries, not inside the mutation handlers.
- `break_glass_admin` stays explicit and separate; it is not one of the routine split roles.
- `read_only_observer` remains a data-domain read-only role; it is not treated as one of the serious operator roles.
- Direct storage-ring access still preserves the current compatibility posture until later workstream slices tighten the `query_session` and admin-data boundary.

## Evidence

- identity permission matrix: `tests/tls_identity.rs::authorization_matrix_enforces_ops_and_observability_boundaries`
- simulator permission guard: `tests/simulator_ladder.rs::quota_authorization_boundary_scenario_is_live`
- control-ring role routing:
  - `tests/control_ring_rpc.rs::status_req_includes_topology_and_committed_lease_metadata`
  - `tests/control_ring_rpc.rs::audit_chain_head_req_returns_local_head_for_operator_auditor_and_observer`
  - `tests/control_ring_rpc.rs::tenant_admin_create_and_list_flow_commits_through_control_ring`
  - `tests/control_ring_rpc.rs::tenant_admin_rotate_tenant_kek_commits_and_returns_new_active_version`
  - `tests/control_ring_rpc.rs::tenant_admin_retire_tenant_kek_removes_safe_historical_version`
  - `tests/control_ring_rpc.rs::tenant_admin_rejects_mutation_for_read_only_or_tenant_scoped_actor`

## Follow-Ons

- freeze the external role/OID claim surface in `F2`
- remove the legacy `operator` compatibility role once cert issuance and onboarding paths stop depending on it
- tighten the admin-versus-storage-ring boundary when `query_session` is live
