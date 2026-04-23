Date: 2026-04-23
Author Role: Lead Architect
Workstream: `F`
Slice: `F5`

# Destructive-Operation Authorization Note

## Objective

Make the destructive-admin boundary explicit in code without forcing a second human into the founder-stage baseline.

## Policy Shape

Wendelon now enforces three explicit administrative categories through `require_administrative_action(...)` in `src/governance.rs`:

- `RoutineAdmin`
- `StrongerAuthorization`
- `BreakGlassOnly`

The default profile remains audited single-operator execution. The important implementation change is that destructive actions no longer rely on ordinary RBAC alone; they flow through an explicit governance gate that later hardened profiles can extend without changing the RPC model.

## Current Action Tiers

### Routine admin

- `tenant_create`
- `tenant_update_quota`
- `tenant_suspend`
- `tenant_resume`
- `revocation_list`

### Stronger authorization

- `tenant_kek_rotate`
- `tenant_kek_retire`
- `revoke_actor_key`
- `transfer_leadership`
- `advance_epoch`
- `join_cluster`
- `cross_ca_promotion`

### Break-glass only

- `tenant_delete`
- `break_glass_access`

## Enforcement Notes

- Tenant-scoped principals are rejected for the administrative action surface instead of being allowed to drift into cluster-wide control paths.
- `TenantAdmin::Delete` now requires a `break_glass_admin` OPS certificate.
- `RevocationAdmin` no longer inherits the broad `ClusterControl` surface in practice:
  - `list` is routine but human-ops-only
  - `revoke` is a stronger-authorization action
- Cluster topology mutations (`transfer`, `advance_epoch`, `join`) still allow `data_node` identities because the same control messages are used by real inter-node control traffic. This slice makes that coupling explicit rather than pretending the path is human-only.

## Non-Goals

- No dual-authorization or pending-cosign workflow yet.
- Break-glass issuance hardening lives in the separate `F6` guide and is not duplicated here.
- No attempt to redesign the cluster-control wire protocol around separate human-only and node-only message types in this slice.

## Evidence

- governance action-matrix proof:
  - `src/governance.rs::tests::administrative_action_policy_distinguishes_allowed_and_restricted_actions`
- destructive tenant delete requires break-glass:
  - `tests/control_ring_rpc.rs::tenant_delete_requires_break_glass_certificate`
- revocation admin rejects non-ops and tenant-scoped callers:
  - `tests/control_ring_rpc.rs::revocation_admin_rejects_non_ops_and_tenant_scoped_callers`
- topology mutation rejects tenant-scoped operator principals:
  - `tests/control_ring_rpc.rs::advance_epoch_rejects_tenant_scoped_operator`

## Follow-Ons

- future hardened profile: dual-auth or multi-party approval layered onto `AdministrativeAction`
