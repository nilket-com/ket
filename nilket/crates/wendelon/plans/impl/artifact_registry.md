# Artifact Registry

Date: 2026-04-22
Author Role: Lead Architect
Primary Owner: `WS3 / D`
Supporting Owner: `WS0 / A`

## Purpose

This file is the durable index for implementation evidence.

It is intentionally simple:

- artifact name
- owning workstream
- canonical path
- gate
- current state

State vocabulary:

- `present`: file exists and is the current canonical location
- `reserved`: path is the agreed future location, but the artifact is not written yet

## Registry

| Artifact | Owner | Canonical Path | Gate | State | Notes |
|---|---|---|---|---|---|
| B transition table | `B` | `plans/impl/workstream_B_transition_table.md` | `M1` | `present` | Write-outcome state model |
| B fault-injection report | `B` | `plans/impl/workstream_B_fault_injection_report.md` | `M1` | `present` | Current injected-failure evidence |
| B replay/idempotency note | `B` | `plans/impl/workstream_B_replay_idempotency_note.md` | `M1` | `present` | Duplicate/restart semantics |
| B operator notes | `B` | `plans/impl/workstream_B_operator_notes.md` | `M1` | `present` | Fenced/degraded operator behavior |
| B write-path benchmark delta | `B` + `D` | `plans/impl/workstream_B_benchmark_delta.md` | `M1` | `reserved` | Harness exists; report does not |
| C admission snapshot note | `C` + `D` | `plans/impl/workstream_C_admission_snapshot_note.md` | `M2` | `reserved` | Canonical note for C1 hot-path state publication |
| C execution-topology note | `C` + `D` | `plans/impl/workstream_C_execution_topology_note.md` | `M2` | `reserved` | WAL/slab rings plus isolated blocking executors |
| C overload-limit matrix | `C` + `D` | `plans/impl/workstream_C_overload_limit_matrix.md` | `M2` | `reserved` | Listener, per-IP, per-connection, and post-auth bounds |
| C benchmark report | `C` + `D` | `plans/impl/workstream_C_benchmark_report.md` | `M2` | `reserved` | Expected to include shared vs split `io_topology` numbers |
| C panic/poison disposition note | `C` + `D` | `plans/impl/workstream_C_panic_poison_note.md` | `M2` | `reserved` | Touched-path panic removals and poison recovery contract |
| D metric catalog | `D` | `plans/impl/workstream_D_metric_catalog.md` | `M1` | `present` | D1 |
| D trace inventory | `D` | `plans/impl/workstream_D_trace_inventory.md` | `M1/M2` | `present` | D2 first-pass live span inventory |
| D chaos matrix | `D` | `plans/impl/workstream_D_chaos_matrix.md` | `M1/M2` | `present` | D3 first-pass matrix and evidence index |
| D simulator registry | `D` | `plans/impl/workstream_D_simulator_registry.md` | `M1/M2` | `present` | D4 complete named scenario registry |
| D property suite note | `D` | `plans/impl/workstream_D_property_suite.md` | `M1/M2` | `present` | D5 minimum property suite |
| D soak specification | `D` | `plans/impl/workstream_D_soak_spec.md` | `M2` | `present` | D6 workload shape and promotion rules |
| D soak report | `D` | `plans/impl/workstream_D_soak_report.md` | `M2/M3` | `present` | D6 report template; measured evidence still pending |
| D dashboard inventory | `D` | `plans/impl/workstream_D_dashboard_inventory.md` | `M2/M3` | `reserved` | Can stay thin at first |
| D alert inventory | `D` | `plans/impl/workstream_D_alert_inventory.md` | `M2/M3` | `reserved` | Can stay thin at first |
| E provider contract note | `E` | `plans/impl/workstream_E_provider_contract.md` | `M0/M3` | `present` | E1 root-KEK bootstrap, health, and rotation seam |
| E local-sealed baseline note | `E` | `plans/impl/workstream_E_local_sealed_baseline.md` | `M3` | `present` | E2 encrypted-at-rest commodity baseline with env/file unlock |
| E production-gating note | `E` | `plans/impl/workstream_E_production_gating.md` | `M3` | `present` | E3 removes the silent `dev-insecure` default and narrows it to explicit dev/test paths |
| E AWS KMS uplift note | `E` | `plans/impl/workstream_E_aws_kms_uplift.md` | `M3` | `present` | E4 lands the first real cloud root-KEK provider without putting KMS on the tenant-KEK hot path |
| E DEK re-wrap sweeper note | `E` | `plans/impl/workstream_E_dek_rewrap_sweeper.md` | `M3` | `present` | E6 converges historical encrypted tiered slabs onto the active tenant KEK in bounded background passes |
| E tenant-KEK retirement note | `E` | `plans/impl/workstream_E_tenant_kek_retirement_accounting.md` | `M3` | `present` | Historical KEK retirement is now derived from replicated tier metadata, surfaced on TenantAdmin, and enforced by an explicit retirement mutation |
| E zeroization evidence note | `E` | `plans/impl/workstream_E_zeroization_note.md` | `M3` | `present` | E7 covers deterministic key scrub tests plus the exact crypto/plaintext buffer boundaries that are actually zeroized today |
| E PKCS#11 seam note | `E` | `plans/impl/workstream_E_pkcs11_seam.md` | `M3` | `present` | E8 lands the stable HSM-facing config/provider seam while keeping the runtime status explicitly unsupported |
| E rotation runbook | `E` | `plans/ops/key_rotation.md` | `M3` | `present` | E5 documents the operator-facing tenant-KEK rotation path and post-commit convergence contract |
| F role matrix | `F` | `plans/impl/workstream_F_role_matrix.md` | `M3` | `present` | F1 lands the four-way serious operator split in code while keeping legacy `operator` as a compatibility alias |
| F claim/OID note | `F` | `plans/impl/workstream_F_claim_oid_note.md` | `M3` | `present` | F2 freezes the placeholder actor-claims OID and the exact four-key CBOR payload shape used in certificates |
| F cert-service deployment note | `F` | `plans/impl/workstream_F_cert_service_deployment_note.md` | `M3` | `present` | F3 lands the narrow issuance-policy baseline for node, operator, observer, break-glass, and `query_session` certificates without turning Wendelon into a general tenant CA |
| F revocation behavior note | `F` | `plans/impl/workstream_F_revocation_behavior_note.md` | `M3` | `present` | F4 lands replicated actor-key revocation through the Control Ring and enforces it on transport plus observability admission |
| F destructive-auth note | `F` | `plans/impl/workstream_F_destructive_auth_note.md` | `M3` | `present` | F5 makes routine, stronger-authorization, and break-glass-only admin actions explicit in code without forcing dual-auth into the default profile |
| F break-glass operational guide | `F` | `plans/impl/workstream_F_break_glass_operational_guide.md` | `M3` | `present` | F6 hardens break-glass issuance and runtime use with a fixed scope taxonomy, mandatory incident metadata, and a dedicated issuance audit event |
| F operator onboarding contract | `F` | `plans/impl/workstream_F_operator_onboarding_contract.md` | `M3` | `present` | F7 freezes the enterprise onboarding boundary: upstream IdP authenticates the human, Wendelon-owned issuance maps to local roles and short-lived certs, and `query_session` remains a separate Workstream I concern |
| G canonical audit schema reference | `G` | `plans/impl/workstream_G_canonical_schema_reference.md` | `M3` | `present` | G1 freezes the first typed audit families for administrative control, local security failures, and data-access evidence without making free-form messages the only machine-readable payload |
| G evidence-path note | `G` | `plans/impl/workstream_G_evidence_path_note.md` | `M3` | `present` | G2 makes the authority-versus-materialization model explicit in code and docs: node-local authority, raft-authoritative materialization, and additive projection sinks |
| G sampling-policy note | `G` | `plans/impl/workstream_G_sampling_policy_note.md` | `M3` | `present` | G3 fixes the pilot data-access audit policy at explicit `100%` emission for the minimum serious dispatcher-visible set instead of implying behavior through whatever emit sites happen to exist |
| G audit verify usage guide | `G` | `plans/impl/workstream_G_audit_verify_usage.md` | `M3` | `present` | G4 documents the baseline local-chain verifier and its contract: chain continuity, malformed evidence detection, signed-root checks, and JSON report / exit-status behavior |
| G archive and retention guide | `G` | `plans/impl/workstream_G_archive_retention_guide.md` | `M3` | `present` | G5 adds the baseline archive mirror on top of the local NDJSON authority, including segmented immutable files, sidecar retention metadata, and explicit operator duties for WORM-backed retention |
| G Kafka evidence-path note | `G` | `plans/impl/workstream_G_kafka_evidence_path_note.md` | `M3` | `present` | G6 keeps Kafka optional but makes canonical off-box chain shipping explicit: distribution mode stays reduced, canonical mode carries the exact persisted local-chain record for replay and verification of delivered records |
| H backup manifest spec | `H` | `plans/impl/workstream_H_backup_manifest_spec.md` | `M3` | `present` | H1 freezes the top-level cluster backup bundle, consistent-cut metadata, and embedded control/storage integrity contract before restore choreography lands |
| H restore runbook | `H` | `plans/impl/workstream_H_restore_runbook.md` | `M3` | `present` | H2 defines the supported restore sequence, remote-tier validation contract, and readiness gating so bootstrap recovery and restored service-readiness stop being conflated |
| H node-replacement runbook | `H` | `plans/impl/workstream_H_node_replacement_runbook.md` | `M3` | `present` | H3 defines the first supported remove-and-replace flow for a fresh replacement identity, including failed-member removal, optional backup seeding, join-mode bootstrap, and post-replacement catch-up verification |
| H fallback automation contract | `H` | `plans/impl/workstream_H_fallback_automation_contract.md` | `M3` | `present` | H4 freezes the external automation contract and points at the executable replacement harness so trigger source, inputs, timeouts, and success checks are no longer tribal knowledge |
| H first-party operator direction | `H` | `plans/impl/workstream_H_first_party_operator_direction.md` | `M3` | `present` | H5 freezes the future Wendelon-owned operator scope as storage-tier-only orchestration over the H2/H3/H4 recovery surfaces, with explicit replacement ownership and drill integration points |
| H runbook catalog | `H` | `plans/ops/runbooks/README.md` | `M3` | `present` | H6 promotes the operator-facing runbook set for replacement, restore, pending-finalization distress, and audit/archive distress out of internal notes and into the durable ops catalog |
| H drill report | `H` | `plans/impl/workstream_H_drill_report.md` | `M3` | `present` | H6 records the required non-author drill evidence shape honestly; the artifact exists, but the gate remains open until a real non-author restore or replacement drill is entered |

## Registry Rules

- If an artifact matters to a gate, it needs a canonical path before the work is “almost done.”
- New evidence should reuse an existing reserved path instead of inventing a new filename late.
- If a reserved artifact becomes obviously unnecessary, delete the row rather than leaving dead placeholders around.
- This registry is allowed to stay small. It should track gate evidence, not every planning note in the repo.
