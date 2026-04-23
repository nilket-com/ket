# Workstream F: Identity, RBAC, Authorization, And Governance

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS5`

## Objective

Turn identity and operator authorization into a coherent system that can survive enterprise scrutiny without becoming an overbuilt PKI product.

## Why Now

The current posture is too broad and too implicit:

- `operator` is too much concentrated power
- cert lifecycle is not yet an explicit product component
- revocation and break-glass cannot stay hand-wavy if other workstreams depend on them

This workstream is what makes operator actions attributable and governable.

## ADR Anchors

- ADR `018`
- ADR `021`
- ADR `031`
- ADR `034`

## Scope

- role-model split
- OID freeze and cert-claim shape
- cert-service implementation plan
- revocation distribution
- destructive-op authorization policy
- break-glass flow
- operator onboarding contract for future IdP integration

## Out Of Scope

- KMS/provider implementation
- audit retention policy
- full Toron compute semantics
- broad tenant-application CA features

## Dependencies

- `A` for claim discipline and ADR alignment
- `E` for custody/provider interactions
- `G` for audit of operator actions
- `I` for the `query_session` boundary and identity handoff

## Entry Criteria

- ADR `018` remains the authority for role and cert scope
- minimum role split is accepted at the design level
- OID/PEN path is tracked even if assignment is still pending

## Implementation Slices

### F1. Role Split In Code

Replace the broad operator role with the minimum serious model:

- `tenant_admin`
- `cluster_admin`
- `backup_admin`
- `compliance_auditor`

The split should be enforced at the dispatcher boundary, not buried inside handlers.

Status: complete.
Artifact: `plans/impl/workstream_F_role_matrix.md`

### F2. OID And Claim Freeze

Freeze the cert extension payload shape:

- role
- scopes
- tenant binding where applicable
- key identity

Once this shape is frozen, external-facing drift stops.

Status: complete.
Artifact: `plans/impl/workstream_F_claim_oid_note.md`

### F3. Cert-Service Baseline

Implement the narrow cert-service scope only:

- node certs
- operator certs
- observer certs
- break-glass certs
- short-lived `query_session` certs

This is not a general tenant-application CA.

Status: complete.
Artifact: `plans/impl/workstream_F_cert_service_deployment_note.md`

### F4. Revocation Distribution

Land revocation distribution through replicated control state so nodes do not depend on a best-effort external refresh path for correctness.

Status: complete.
Artifact: `plans/impl/workstream_F_revocation_behavior_note.md`

### F5. Destructive-Operation Authorization

Introduce stronger authorization requirements for destructive operations.

The code path should be explicit about:

- what is routine admin
- what requires stronger approval
- what is break-glass only

Status: complete.
Artifact: `plans/impl/workstream_F_destructive_auth_note.md`

### F6. Break-Glass Path

Implement the exceptional path as:

- explicit
- short-lived
- narrow in scope
- visibly audited

Break-glass is allowed, but it should feel expensive and loud.

Status: complete.
Artifact: `plans/impl/workstream_F_break_glass_operational_guide.md`

### F7. Operator Onboarding Contract

Document the minimal mapping contract for future enterprise operator onboarding:

- where external identity maps into Wendelon roles
- where local cert issuance begins
- what is out of scope for the first pilot

Status: complete.
Artifact: `plans/impl/workstream_F_operator_onboarding_contract.md`

## Tests And Benchmarks

- dispatcher authorization matrix tests
- cert issuance and renewal tests
- revocation propagation tests with stale-node scenarios
- break-glass issuance and expiry tests
- destructive-op policy tests
- issuance load tests for short-lived `query_session` certs

## Exit Criteria

- the broad legacy operator role is no longer the only serious control model
- cert claim semantics are frozen enough for dependent systems to integrate against them
- revocation works through the replicated control surface
- destructive operations have stronger authorization than routine operations
- break-glass is real, narrow, and auditable

## Artifacts

- role matrix
- cert-claim/OID note
- cert-service deployment note
- revocation behavior note
- destructive-auth note
- break-glass operational guide
- operator onboarding contract

## Deferred Follow-Ons

- richer delegated administration models
- broader external IdP integration breadth
- general tenant-application PKI capabilities
- more elaborate approval workflows if customer demand justifies them
