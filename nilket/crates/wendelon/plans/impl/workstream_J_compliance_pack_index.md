# Workstream J Compliance-Pack Index

Date: 2026-04-23
Owner: `J`
Status: Skeleton present; regulated GA evidence incomplete

## Purpose

This is the minimum index for regulated pilot and regulated GA conversations. It points to repo-visible evidence only; it does not replace external lab records, release receipts, or customer-specific security questionnaires.

## Package Index

| Section | Evidence | Status |
|---|---|---|
| Crypto-boundary summary | `plans/impl/workstream_J_crypto_boundary_inventory.md` | present |
| FIPS build profile | `plans/impl/workstream_J_fips_build_profile.md` | present |
| Startup self-tests | `plans/impl/workstream_J_self_test_evidence.md` | present |
| Release artifacts | `plans/impl/workstream_J_release_artifact_evidence.md` | partial; scripts exist, final outputs absent |
| CMVP status | `plans/impl/workstream_J_cmvp_status_evidence.md` | partial; external dependency open |
| External lab contact | `plans/impl/external/cmvp_lab_contact_record.md` | open |
| Claim discipline | `plans/impl/claim_discipline.md` | present |
| Custody provider posture | `plans/impl/workstream_E_provider_contract.md`, `plans/impl/workstream_E_local_sealed_baseline.md`, `plans/impl/workstream_E_aws_kms_uplift.md`, `plans/impl/workstream_E_pkcs11_seam.md` | present with provider-specific caveats |
| Identity and operator authorization | `plans/impl/workstream_F_role_matrix.md`, `plans/impl/workstream_F_cert_service_deployment_note.md`, `plans/impl/workstream_F_break_glass_operational_guide.md` | present |
| Audit evidence | `plans/impl/workstream_G_canonical_schema_reference.md`, `plans/impl/workstream_G_evidence_path_note.md`, `plans/impl/workstream_G_audit_verify_usage.md`, `plans/impl/workstream_G_archive_retention_guide.md` | present |
| Recovery and restore | `plans/impl/workstream_H_backup_manifest_spec.md`, `plans/impl/workstream_H_restore_runbook.md`, `plans/impl/workstream_H_node_replacement_runbook.md`, `plans/impl/workstream_H_drill_report.md` | partial; real non-author drill still open |

## Regulated Conversation Baseline

The compliance pack may support technical due diligence. It must not be presented as a completed regulated GA binder until:

- CMVP/lab state is recorded externally and reflected in claim discipline
- FIPS transport and non-aws-lc signature/KDF gaps are resolved or scoped by the lab
- release evidence includes an SPDX SBOM, detached signature, and attestation output for the exact binary
- hostile-failure and non-author recovery drill evidence are complete where the buyer requires them

## Maintenance Rule

Any scorecard row that moves to `done` for `M5 Regulated GA` must update this index in the same change set.
