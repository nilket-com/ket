Date: 2026-04-23
Author Role: Lead Architect
Workstream: `F`
Slice: `F7`

# Operator Onboarding Contract

## Objective

Define the minimum contract for enterprise operator onboarding without turning Wendelon into a general-purpose IdP, token broker, or customer PKI platform.

## Core Boundary

Wendelon does **not** accept enterprise bearer tokens as its primary storage-ring or coordination-ring credential.

The contract is:

1. An external enterprise identity system authenticates the human.
2. A narrow onboarding/issuance component evaluates that identity against role-mapping policy.
3. That component issues a short-lived Wendelon operator certificate carrying the frozen F2 claims.
4. Wendelon authorizes the resulting mTLS identity directly.

So the authoritative split is:

- external IdP owns human authentication
- Wendelon-owned issuance policy owns role translation into Wendelon certs
- Wendelon runtime owns mTLS authorization and revocation enforcement

## Supported Role Targets

The serious onboarding targets are:

- `tenant_admin`
- `cluster_admin`
- `backup_admin`
- `compliance_auditor`

Additional non-routine targets:

- `read_only_observer`
- `break_glass_admin`

Compatibility note:

- legacy `operator` may still exist in transitional deployments
- it is not the preferred serious onboarding target
- new enterprise integrations should map to the split-role model first

## Required Claim Output

Issued operator certificates must use the frozen actor-claims payload from F2:

- `role`
- `scopes`
- `tenant_id`
- `key_id`

For cluster-wide operator roles:

- `tenant_id = null`

For normal operator onboarding, baseline scopes remain the narrow role-defaults already fixed in F3:

- routine operator/admin certs: `["cluster"]`
- break-glass: use the F6 break-glass scope taxonomy instead of a generic cluster scope

## Where Local Issuance Begins

The onboarding line is crossed when an upstream human identity is translated into one of the Wendelon roles above.

Before that line, the identity is external and enterprise-specific.
After that line, the identity is a Wendelon certificate subject to:

- short-lived TTL limits
- Wendelon revocation behavior
- Wendelon audit attribution
- Wendelon dispatcher/governance policy

This means the cert-service or its future equivalent is the **translation seam**, not a pass-through token relay.

## Minimal Mapping Contract

The onboarding adapter must be able to answer exactly these questions:

1. Which Wendelon role should this human receive?
2. Is the binding cluster-wide or tenant-scoped?
3. What is the stable upstream human identifier for issuance/audit correlation?
4. What key material or CSR is being certified?
5. What is the requested lifetime, and does it fit the Wendelon maximum for that leaf class?

Everything else is optional integration detail.

## First-Pilot Scope

The first serious pilot contract is intentionally narrow:

- one major enterprise IdP path, customer-chosen in practice
  - for example: Okta or Azure AD
- one generic SAML-compatible path
- software-backed cert issuance is acceptable
- revocation may be short-lifetime-first rather than real-time IdP push

This is enough to prove:

- role mapping is authoritative
- issuance is controlled by local Wendelon policy
- offboarding is bounded by cert lifetime and explicit revocation support

A second first-class IdP is customer-driven, not mandatory baseline scope.

## Explicit Non-Goals For The First Pilot

F7 does **not** commit Wendelon to:

- accepting upstream JWT/OIDC/SAML tokens directly on the storage ring or coordination ring
- acting as a general tenant-application CA
- issuing arbitrary customer workload certificates
- becoming a universal IdP federation product
- solving per-user `query_session` handoff here

That last item belongs to Workstream `I`, where the Toron/Wendelon user-session bridge is versioned as a separate compute-storage boundary.

## Operational Consequences

- Offboarding a human from the upstream IdP does not automatically bypass Wendelon TTL limits; short-lived operator certs remain the baseline exposure bound.
- Optional upstream-triggered revocation hooks are allowed later, but they are a hardening layer on top of replicated revocation, not a replacement for it.
- Customer operators should think in terms of “issue short-lived Wendelon certs from upstream identity,” not “teach Wendelon to speak every enterprise auth protocol directly.”

## Evidence And Cross-References

Code and artifacts this contract rests on:

- F1 role split: `plans/impl/workstream_F_role_matrix.md`
- F2 frozen claim shape: `plans/impl/workstream_F_claim_oid_note.md`
- F3 issuance baseline: `plans/impl/workstream_F_cert_service_deployment_note.md`
- F4 revocation distribution: `plans/impl/workstream_F_revocation_behavior_note.md`
- F5 destructive admin boundary: `plans/impl/workstream_F_destructive_auth_note.md`
- F6 break-glass issuance/runtime contract: `plans/impl/workstream_F_break_glass_operational_guide.md`
- ADR `018`
- ADR `034` for the separate `query_session` boundary

## Follow-Ons

- customer-specific role-mapping config format
- first real onboarding adapter against one major IdP
- generic SAML reference path
- explicit issuance API shape for a future `wendelon-cert-service`
