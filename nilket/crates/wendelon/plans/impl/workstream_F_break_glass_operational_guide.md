Date: 2026-04-23
Author Role: Lead Architect
Workstream: `F`
Slice: `F6`

# Break-Glass Operational Guide

## Objective

Make break-glass concrete enough that it is constrained by code instead of by operator folklore.

## What F6 Lands

Break-glass now has a real baseline contract in code:

- issuance is still single-operator in the default profile
- TTL remains capped at `15m`
- issuance requires:
  - exactly one explicit scope
  - a non-empty `incident_ticket_id`
  - a non-empty human `reason`
- the issued scope is enforced at runtime
- issuance has a dedicated audit event shape

This does **not** yet add a network-facing issuance service. It hardens the issuance policy engine and the runtime authorization boundary that a future cert-service binary will use.

## Scope Taxonomy

Break-glass scopes are now fixed to this small set:

- `diagnostic`
- `tenant_ops`
- `key_ops`
- `cluster_ops`
- `full`

The baseline posture is intentionally narrow:

- `diagnostic`
  - observability `/ready`
  - observability `/metrics`
  - audit read
  - control/status read surfaces
- `tenant_ops`
  - tenant read
  - tenant create/update/suspend/resume
- `key_ops`
  - tenant KEK rotate/retire
- `cluster_ops`
  - cluster-control operations
  - actor-key revocation admin
- `full`
  - all of the above
  - required for explicit break-glass-only actions such as `tenant_delete`

## Issuance Rules

Break-glass issuance in `src/network/cert_issuance_policy.rs` now requires:

- `key_id` present
- TTL `> 0` and `<= 15m`
- one explicit `BreakGlassScope`
- `incident_ticket_id`
  - trimmed
  - length-bounded
  - restricted to operator-friendly ticket characters
- `reason`
  - trimmed
  - length-bounded
  - no control characters

The frozen actor-claims payload remains unchanged:

- `role`
- `scopes`
- `tenant_id`
- `key_id`

Incident metadata stays out of the cert claim and instead belongs to issuance audit evidence.

## Audit Behavior

F6 adds a dedicated issuance audit event:

- `identity.break_glass_issued`

The canonical event carries:

- the issuing actor
- `break_glass` outcome
- the issued scope
- `incident_ticket_id`
- `reason`
- TTL in seconds

This keeps the issuance path visibly distinct from ordinary operator certificate activity.

## Runtime Enforcement

Break-glass scope is no longer descriptive-only. `authorize_actor(...)` and the F5 governance gate now enforce it.

Important consequences:

- a `diagnostic` break-glass cert can scrape metrics but cannot mutate tenant state
- a `key_ops` break-glass cert can rotate/retire tenant KEKs but cannot perform cluster control
- `tenant_delete` now needs both:
  - `break_glass_admin`
  - `full` scope

## Evidence

- issuance validation:
  - `src/network/cert_issuance_policy.rs::tests::break_glass_requires_reason_ticket_scope_and_short_ttl`
- issuance audit shape:
  - `src/network/cert_issuance_policy.rs::tests::break_glass_issue_is_single_scope_and_builds_audit_event`
- runtime scope enforcement:
  - `tests/tls_identity.rs::break_glass_scope_matrix_is_enforced`
- destructive control path:
  - `tests/control_ring_rpc.rs::tenant_delete_requires_break_glass_certificate`
- observability path:
  - `tests/observability_listeners.rs::observability_rejects_wrong_ca_domains_and_marks_break_glass_audit`

## Deferred Follow-Ons

- dual-authorization for broad-scope issuance in hardened deployments
- real issuance RPC/HTTP service
- richer incident metadata schema if the pilot actually needs it
