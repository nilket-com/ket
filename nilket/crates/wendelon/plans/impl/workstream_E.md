# Workstream E: Crypto Custody And Key Lifecycle

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS4`

## Objective

Replace the development-only custody posture with a provider-based production key story that works on commodity deployments and scales upward to stronger hardening profiles.

## Why Now

The current system cannot keep `dev-insecure` on the serious path and still claim a credible customer pilot.

This workstream must land early enough that:

- backup encryption
- tenant KEK lifecycle
- audit of key events
- regulated-path planning

all build on the same provider stack instead of inventing their own.

## ADR Anchors

- ADR `020`
- ADR `031`
- ADR `026`

## Scope

- Root KEK provider abstraction
- `local-sealed` baseline provider
- at least one cloud-KMS provider
- `dev-insecure` production gating
- tenant KEK rotation RPC
- DEK re-wrap sweeper
- key-age enforcement hooks
- plaintext zeroization in sensitive paths
- provider readiness and failure semantics

## Out Of Scope

- full enterprise IdP flow
- audit retention policy
- final CMVP packaging
- broad tenant-application PKI

## Dependencies

- `A` for status and claim discipline
- `F` for authz on rotation and operator flows
- `G` for audit of key events
- `J` for FIPS-boundary coordination

## Entry Criteria

- ADR `031` is the authority for provider layering
- unlock contract for `local-sealed` is design-locked
- production configuration policy for `dev-insecure` is agreed

## Implementation Slices

### E1. Provider Contract

Create the provider interface for:

- generate or enroll
- wrap
- unwrap
- rotate
- health/readiness

Higher layers should not care whether the Root KEK lives in a sealed local store, cloud KMS, or later PKCS#11.

### E2. `local-sealed` Baseline

Implement the default serious deployment path:

- encrypted-at-rest local root material
- explicit unlock at startup
- support for:
  - operator-supplied secret
  - platform secret injection

This is the commodity-default baseline.

### E3. Production Gating

Make `dev-insecure` impossible to use accidentally in serious deployments:

- explicit config only
- loud warnings in development
- fail-fast in production profiles

### E4. Cloud-KMS Uplift

Ship at least one real cloud provider first.

Preferred first landing:

- AWS KMS

Design for quick follow-on providers behind the same contract:

- GCP KMS
- Azure Key Vault

### E5. Tenant KEK Rotation

Add a real rotation surface with:

- authn/authz
- audit hooks
- restart-safe behavior
- compatibility with future stronger approval policies

### E6. DEK Re-Wrap Sweeper

Implement the re-wrap path so tenant-key rotation does not leave old wrapped DEKs indefinitely stranded.

### E7. Zeroization And Sensitive-Buffer Hygiene

Identify sensitive plaintext/key-holding paths and add:

- zeroize-on-drop or equivalent
- deterministic test coverage where possible
- clear boundaries on what is and is not guaranteed

### E8. PKCS#11 Seam

Do not build a vendor maze yet, but do preserve the seam for:

- enterprise HSM
- regulated uplift

without reshaping the provider contract later.

## Tests And Benchmarks

- provider conformance tests
- unlock and restart tests for `local-sealed`
- negative tests for production rejection of `dev-insecure`
- rotation integration tests
- re-wrap sweeper recovery tests
- zeroization harness for controlled deterministic coverage
- cold-read latency benchmark for provider unwrap behavior
- outage/distress tests for provider failure handling

## Exit Criteria

- a non-`dev-insecure` default production path exists
- `local-sealed` works as the commodity baseline
- at least one cloud-KMS provider works behind the same contract
- rotation and re-wrap paths are real, audited, and restart-safe
- sensitive-path zeroization coverage is explicit and tested where practical

## Artifacts

- provider contract note
- custody threat-model note
- production-config policy for provider selection
- rotation runbook
- zeroization evidence note

## Deferred Follow-Ons

- multi-cloud provider breadth beyond the first real provider
- PKCS#11 vendor-specific hardening
- escrow ceremony details
- stronger regulated-profile operational controls layered above the same provider stack
