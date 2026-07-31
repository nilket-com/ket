# Compliance-Pack Index

Date: 2026-04-23
Owner: `J`
Status: Skeleton present; regulated GA evidence incomplete

## Purpose

This is the minimum index for regulated pilot and regulated GA conversations. It points to repo-visible evidence only; it does not replace external lab records, release receipts, or customer-specific security questionnaires.

## Package Index

| Section | Evidence | Status |
|---|---|---|
| Crypto-boundary summary | `plans/compliance/crypto_boundary_inventory.md` | present |
| FIPS build profile | `plans/compliance/fips_build_profile.md` | present |
| Startup self-tests | `plans/compliance/self_test_evidence.md` | present |
| Release artifacts | `plans/compliance/release_artifact_evidence.md` | partial; scripts exist, final outputs absent |
| CMVP status | `plans/compliance/cmvp_status_evidence.md` | partial; external dependency open |
| External lab contact | `plans/compliance/external/cmvp_lab_contact_record.md` | open |
| Claim discipline | `plans/governance/claim_discipline.md` | present |
| Custody provider posture | ADR `031`, `plans/ops/key_rotation.md`, implemented config/provider code | present with provider-specific caveats |
| Identity and operator authorization | ADR `018`, TLS/authorization code, identity tests | present |
| Audit evidence | `plans/ops/audit.md`, audit verifier tests, audit bundle tests | present |
| Recovery and restore | `plans/ops/runbooks/cluster_restore.md`, `plans/ops/runbooks/node_replacement.md`, backup/restore and multinode tests | partial; real non-author drill still open |

## Regulated Conversation Baseline

The compliance pack may support technical due diligence. It must not be presented as a completed regulated GA binder until:

- CMVP/lab state is recorded externally and reflected in claim discipline
- FIPS transport and non-aws-lc signature/KDF gaps are resolved or scoped by the lab
- release evidence includes an SPDX SBOM, detached signature, and attestation output for the exact binary
- hostile-failure and non-author recovery drill evidence are complete where the buyer requires them

## Maintenance Rule

Any regulated-GA evidence status change must update this index in the same change set.
