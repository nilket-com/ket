# Workstream E: Crypto Custody And Key Lifecycle

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS4

## Mandate

Replace the development-only custody model with a production one that a real customer security team can accept.

## Architectural Position

`dev-insecure` is development scaffolding, not a shippable option. The first serious pilot cannot be built on a plaintext root-KEK stored on local disk.

My position is:

- cloud KMS support lands first
- insecure custody becomes mechanically impossible in default production profiles
- tenant KEK rotation is not "later"; it is part of the minimum production story
- plaintext zeroization is a real requirement, not compliance decoration
- maximum tenant-KEK age should be enforced by the system, not left as an operator suggestion

## Scope

- cloud KMS providers
- PKCS#11/HSM-compatible provider path
- M-of-N root-KEK escrow design
- tenant KEK rotation RPC
- DEK re-wrap sweeper
- maximum tenant-KEK age enforcement
- plaintext buffer zeroization
- production config gating

## Non-Goals

- full enterprise IdP flow
- transport FIPS boundary
- audit retention and evidence packaging

## Work Packages

### E1. Production KMS First

Ship at least one cloud KMS provider immediately, with a structure that supports:

- AWS KMS
- GCP KMS
- Azure Key Vault

My bias for first landing is AWS first, then GCP/Azure quickly behind it, because one real provider is better than three partial abstractions.

For regulated pilot planning, this workstream should still name all four target providers explicitly:

- AWS KMS
- GCP Cloud KMS
- Azure Key Vault
- PKCS#11/HSM

I do not require all four before the first customer-facing pilot, but I do require the interface and program plan to point at all four rather than pretending one provider is the final story.

### E2. Production Gating

Make `dev-insecure`:

- explicit
- dev/test only
- impossible to load in default production profiles

Fail fast on startup. Do not let the product boot into an obviously bad posture and then tell the operator they should know better.

### E3. Rotation Surface

Add a real tenant KEK rotation RPC and operator flow.

Requirements:

- authenticated and authorized
- auditable
- restart-safe
- consistent with the future dual-auth model

### E4. DEK Re-Wrap Sweeper

The system must be able to converge old slabs onto the current tenant KEK without operator heroics.

Requirements:

- bounded throughput controls
- progress visibility
- completion criteria tied to KEK retirement

### E5. Escrow And HSM Path

Design the root-KEK escrow process and the generic HSM path.

I do not require on-prem HSM completion for the first customer-facing pilot, but I do require the interface and ceremony design to exist before we start claiming enterprise custody maturity.

Preferred ceremony shape:

- Shamir `3-of-5`
- HSM-sealed shares
- scheduled recombine verification rather than a ceremony nobody has ever practiced

### E6. Maximum Tenant-KEK Age

Add enforcement for maximum tenant-KEK age at a durable enforcement point.

My preference is:

- reads continue
- writes are rejected once the active tenant KEK exceeds policy
- operators can extend only through an explicit audited override

### E7. Plaintext Hygiene

Review and fix plaintext buffer lifetime in:

- slab reads
- re-wrap paths
- any decrypt-and-reencrypt flow

## Deliverables

- production root-KEK provider(s)
- production startup gating
- tenant KEK rotation RPC
- DEK re-wrap sweeper
- escrow design note
- HSM integration design or implementation
- maximum tenant-KEK age policy and enforcement point
- zeroization review note and fixes

## Exit Criteria

- a real production custody path exists in code
- `dev-insecure` is no longer a default or silent option
- tenant KEK rotation is testable end to end
- retired tenant KEKs can be drained by the re-wrap sweeper to zero live references
- maximum tenant-KEK age is enforced or explicitly overridden through an audited path
- plaintext handling in the touched paths is explicitly reviewed

## Recommended Sequencing

1. first cloud KMS provider
2. production startup gating
3. rotation RPC
4. DEK re-wrap sweeper
5. escrow design
6. maximum tenant-KEK age enforcement
7. PKCS#11/HSM path
8. zeroization cleanup

## Risks

- provider abstraction becomes too generic before any provider is truly working
- re-wrap sweeper adds operational load without enough observability
- zeroization fixes interact badly with buffer reuse and performance assumptions
- provider-count expectations for regulated customers outpace the pace at which the team can harden each integration

## Opinionated Constraints

- Do not build a giant KMS abstraction before one provider actually works end to end.
- Do not accept "dev only in practice" as a substitute for hard production gating.
- Do not ship rotation without a convergence story for old DEK wraps.
- Do not leave key-age policy entirely in documentation; if age matters, the state machine or write path must enforce it.

## Open Questions

- Is one cloud KMS provider enough for Gate 3, or do we require all three major clouds before the first customer-facing pilot?
- Do we need PKCS#11 support before regulated pilot, or is a documented roadmap sufficient?
- What exact evidence do we require to say plaintext buffers are adequately zeroized?
- How do we version and audit escrow ceremonies so they are operationally repeatable rather than person-dependent?
- Should maximum tenant-KEK age be enforced at state-machine apply, write admission, or both?
