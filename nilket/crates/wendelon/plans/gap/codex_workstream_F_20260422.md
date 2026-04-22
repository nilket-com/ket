# Workstream F: Identity, RBAC, Authorization, And Governance

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS5

## Mandate

Turn the current pilot-grade auth model into one that survives an enterprise procurement and incident review.

## Architectural Position

The current `operator` role is too broad. That is not a documentation problem; it is a product problem.

My position is:

- role split happens before serious pilot use
- destructive operations require stronger authorization than routine operations
- break-glass remains possible, but loud, narrow, and auditable
- cert lifecycle and revocation must be treated as first-class system components, not side notes
- the cert-rotation service should be a standalone HA component, not a hidden feature inside the cluster it certifies
- CRL distribution should prefer state-machine distribution over an external endpoint if we can keep it simple

## Scope

- enterprise OID freeze after PEN assignment
- cert-rotation service design and implementation
- CRL distribution posture
- role split
- dual-authorization for destructive operations
- break-glass hardening
- IdP integration direction

## Non-Goals

- KMS provider implementation
- full SIEM/audit packaging
- Toron SQL/BI surface

## Required Role Model

Minimum split:

- `tenant_admin`
- `cluster_admin`
- `backup_admin`
- `compliance_auditor`

The current single `operator` role is too much concentrated authority for a serious pilot.

I am intentionally holding the split at these four roles. More roles make the matrix harder to reason about before we have proven the basic model.

## Work Packages

### F1. OID Freeze

As soon as the PEN is assigned:

- freeze the OID
- document it
- stop any drift in cert encoding

No external customer integration should happen before this is fixed.

### F2. Cert-Rotation Service

Design and ship a dedicated cert-rotation service with:

- explicit HA posture
- no hidden dependency on the cluster it is certifying
- clear trust-anchor ownership

Preferred posture:

- standalone binary
- its own HA control plane
- no bootstrap deadlock where expired cluster certs prevent cert renewal

### F3. Revocation Story

Choose and implement the CRL posture.

My bias: use state-machine-distributed CRL data if the implementation stays operationally simple. A clever external revocation service with its own hidden HA burden is not an improvement.

### F4. Role Split And Policy Rewrite

Update:

- role enum
- required-role declarations
- docs and runbooks
- test matrix

### F5. Dual-Authorization

Apply stronger authorization to destructive flows:

- tenant deletion
- key destruction
- membership operations with quorum or durability impact
- break-glass issuance

This should be enforced at proposal validation and re-validated at the durable commit boundary, not just at the API edge.

### F6. Break-Glass Hardening

Requirements:

- explicit reason
- incident/ticket reference
- narrow scope where possible
- high-visibility audit
- dual-authorization on issuance for the truly broad cases

### F7. IdP Mapping

Define how enterprise operator identity maps into cert issuance.

This does not need to be the first implementation milestone, but it does need a real plan before regulated pilot.

## Deliverables

- frozen OID specification
- cert-rotation service
- pending-cosign workflow for destructive proposals
- revocation design and implementation
- split role model
- dual-authorization implementation
- break-glass workflow
- IdP integration design note

## Exit Criteria

- the broad `operator` role is retired from serious deployments
- destructive operations are guarded by stronger authorization policy
- cert lifecycle is operationally owned and testable
- break-glass is narrow and auditable rather than informal
- revocation distribution is explicit enough that staleness and failure modes are testable

## Recommended Sequencing

1. OID freeze
2. cert-rotation design
3. role split
4. CRL implementation
5. dual-auth
6. break-glass hardening
7. IdP mapping

## Risks

- dual-auth slows incident response if poorly scoped
- cert service becomes a new hidden single point of failure
- role split lands in code but not in operational practice
- revocation distribution becomes over-engineered and harder to operate than the problem warrants

## Opinionated Constraints

- Do not ship regulated-pilot posture with the current `operator` role.
- Do not push destructive-op protections down into runbook convention; enforce them in the system.
- Do not let break-glass become a general-purpose bypass that everyone quietly depends on.
- Do not embed cert lifecycle inside the product in a way that deadlocks bootstrap or renewal.

## Open Questions

- Which operations absolutely require dual-auth, and which can remain single-auth with strong audit?
- Should break-glass issuance itself require dual-auth, or only its high-risk uses?
- What revocation distribution model is simplest while still meeting the fail-closed posture?
- How far do we need to go on IdP integration before the first regulated pilot?
- Does the cert-rotation service need its own consensus substrate, or can we keep HA without introducing a second full distributed control plane?
