# Implementation Scorecard

Date: 2026-04-22
Author Role: Lead Architect
Primary Owner: `WS3 / D`
Supporting Owner: `WS0 / A`

## Purpose

This is the minimum scorecard needed to keep implementation evidence honest.

It exists to answer three questions quickly:

1. what artifact is required for a gate
2. where that artifact lives
3. whether it is actually done

Status vocabulary:

- `done`: artifact exists and is usable now
- `partial`: artifact exists but is not yet enough to satisfy the gate
- `open`: artifact does not exist or is not trustworthy yet

## Gate Rows

| Gate | Row | Status | Owner | Artifact / Source | Notes |
|---|---|---|---|---|---|
| `M0 Foundation` | ADR wave accepted/amended through `034` | `done` | `A` | `plans/adr/003,009,011,018,020,021,025,027,029,030-034` | Current architecture baseline |
| `M0 Foundation` | Design-lock register | `done` | `A` | `plans/gap/design_lock_20260422.md` | Program-of-record narrow decisions |
| `M0 Foundation` | Implementation roadmap | `done` | `A` | `plans/impl/roadmap.md` | Current milestone/dependency map |
| `M0 Foundation` | Artifact registry | `done` | `D` | `plans/impl/artifact_registry.md` | Minimal `A-lite` support for `D` |
| `M0 Foundation` | Scorecard | `done` | `D` | `plans/impl/scorecard.md` | This file |
| `M0 Foundation` | E provider contract note | `done` | `E` | `plans/impl/workstream_E_provider_contract.md` | E1 freezes the root-KEK provider seam before `local-sealed` and cloud-KMS land |
| `M3 Customer Pilot Ready` | E local-sealed baseline note | `done` | `E` | `plans/impl/workstream_E_local_sealed_baseline.md` | E2 lands the first non-`dev-insecure` custody path with env/file unlock |
| `M3 Customer Pilot Ready` | E production-gating note | `done` | `E` | `plans/impl/workstream_E_production_gating.md` | E3 makes `local-sealed` the ordinary config default and keeps `dev-insecure` explicit-only |
| `M3 Customer Pilot Ready` | E AWS KMS uplift note | `done` | `E` | `plans/impl/workstream_E_aws_kms_uplift.md` | E4 lands the first real cloud custody uplift and keeps KMS off the tenant-KEK hot path |
| `M3 Customer Pilot Ready` | E DEK re-wrap sweeper note | `done` | `E` | `plans/impl/workstream_E_dek_rewrap_sweeper.md` | E6 makes tenant-KEK rotation converge historical encrypted tiered slabs through a bounded background rewrite pass |
| `M3 Customer Pilot Ready` | E tenant-KEK retirement note | `done` | `E` | `plans/impl/workstream_E_tenant_kek_retirement_accounting.md` | Historical KEK retirement is now measured from replicated tier metadata and enforced through an explicit Control Ring retirement mutation |
| `M3 Customer Pilot Ready` | E zeroization evidence note | `done` | `E` | `plans/impl/workstream_E_zeroization_note.md` | E7 narrows zeroization to actual secret material and transient re-encryption buffers, with explicit non-goals for broader plaintext payloads |
| `M3 Customer Pilot Ready` | E PKCS#11 seam note | `done` | `E` | `plans/impl/workstream_E_pkcs11_seam.md` | E8 lands a stable `pkcs11` config/provider seam and explicit unsupported status without shipping a fake HSM backend |
| `M3 Customer Pilot Ready` | E rotation runbook | `done` | `E` | `plans/ops/key_rotation.md` | E5 now has a live Control Ring operator surface and documents the convergence/audit contract |
| `M3 Customer Pilot Ready` | F role matrix | `done` | `F` | `plans/impl/workstream_F_role_matrix.md` | F1 lands the four-way serious operator split at dispatcher boundaries while preserving legacy `operator` compatibility |
| `M3 Customer Pilot Ready` | F claim/OID note | `done` | `F` | `plans/impl/workstream_F_claim_oid_note.md` | F2 freezes the placeholder actor-claims OID and the exact four-key CBOR claim shape, including explicit `tenant_id = null` for cluster-wide actors |
| `M3 Customer Pilot Ready` | F cert-service deployment note | `done` | `F` | `plans/impl/workstream_F_cert_service_deployment_note.md` | F3 lands the narrow cert-service issuance baseline, including `query_session`, CA split, and pilot TTL limits without expanding into a general tenant-application CA |
| `M3 Customer Pilot Ready` | F revocation behavior note | `done` | `F` | `plans/impl/workstream_F_revocation_behavior_note.md` | F4 replicates actor-key revocation through the Control Ring and enforces it on transport plus observability admission without depending on external CRL refresh |
| `M3 Customer Pilot Ready` | F destructive-auth note | `done` | `F` | `plans/impl/workstream_F_destructive_auth_note.md` | F5 makes the routine-versus-destructive-versus-break-glass admin boundary explicit in code and rejects tenant-scoped or non-ops callers from the destructive control paths |
| `M3 Customer Pilot Ready` | F break-glass operational guide | `done` | `F` | `plans/impl/workstream_F_break_glass_operational_guide.md` | F6 fixes the break-glass scope taxonomy, mandatory issuance metadata, runtime scope enforcement, and the dedicated `identity.break_glass_issued` audit shape |
| `M3 Customer Pilot Ready` | F operator onboarding contract | `done` | `F` | `plans/impl/workstream_F_operator_onboarding_contract.md` | F7 fixes the enterprise onboarding boundary and keeps upstream human identity, local cert issuance, and the separate `query_session` bridge from drifting together |
| `M3 Customer Pilot Ready` | G canonical audit schema reference | `done` | `G` | `plans/impl/workstream_G_canonical_schema_reference.md` | G1 freezes the minimum typed audit families for admin/control mutations, local security failures, and data-access evidence so free-form messages stop being the only durable machine-readable contract |
| `M3 Customer Pilot Ready` | G evidence-path note | `done` | `G` | `plans/impl/workstream_G_evidence_path_note.md` | G2 makes the authority-versus-materialization split explicit in runtime records, sink disposition, and operator docs so local NDJSON stays the serious-deployment baseline and off-box sinks stay projections |
| `M3 Customer Pilot Ready` | G sampling-policy note | `done` | `G` | `plans/impl/workstream_G_sampling_policy_note.md` | G3 makes the pilot data-access audit policy explicit: the dispatcher-visible minimum serious set currently emits at `100%`, and anything more selective is deferred until tenant policy and retention economics are real |
| `M3 Customer Pilot Ready` | G audit verify usage guide | `done` | `G` | `plans/impl/workstream_G_audit_verify_usage.md` | G4 lands the baseline verifier for local NDJSON evidence and documents the operator path, output contract, and current non-goals instead of leaving chain replay as a library-only helper |
| `M3 Customer Pilot Ready` | G archive and retention guide | `done` | `G` | `plans/impl/workstream_G_archive_retention_guide.md` | G5 adds the baseline archive mirror for the local NDJSON chain, documents the segmented immutable-file contract and sidecar retention metadata, and makes the operator WORM duties explicit instead of hand-waving them as “future archive work” |
| `M3 Customer Pilot Ready` | G Kafka evidence-path note | `done` | `G` | `plans/impl/workstream_G_kafka_evidence_path_note.md` | G6 keeps Kafka optional but makes the payload choice explicit: default distribution mode stays reduced, while canonical mode ships the exact persisted local-chain record so delivered Kafka messages can be replayed and verified off-box |
| `M1 Correctness Locked` | B transition table | `done` | `B` | `plans/impl/workstream_B_transition_table.md` | |
| `M1 Correctness Locked` | B fault-injection report | `done` | `B` | `plans/impl/workstream_B_fault_injection_report.md` | |
| `M1 Correctness Locked` | B replay/idempotency note | `done` | `B` | `plans/impl/workstream_B_replay_idempotency_note.md` | |
| `M1 Correctness Locked` | B operator note | `done` | `B` | `plans/impl/workstream_B_operator_notes.md` | |
| `M1 Correctness Locked` | B write-path benchmark delta | `open` | `B` + `D` | reserved in `artifact_registry.md` | Harness exists; measured delta report does not |
| `M1 Correctness Locked` | D metric catalog | `done` | `D` | `plans/impl/workstream_D_metric_catalog.md` | D1 output |
| `M1 Correctness Locked` | D trace inventory | `done` | `D` | `plans/impl/workstream_D_trace_inventory.md` | D2 first-pass live span inventory is on disk |
| `M1 Correctness Locked` | D chaos matrix | `done` | `D` | `plans/impl/workstream_D_chaos_matrix.md` | Matrix is on disk and every planned first-pass row is now live, including isolated-leader partition-plus-restart |
| `M1 Correctness Locked` | D simulator registry | `done` | `D` | `plans/impl/workstream_D_simulator_registry.md` | D4 registry is complete and every planned simulator row is now live |
| `M1 Correctness Locked` | D property suite | `done` | `D` | `plans/impl/workstream_D_property_suite.md`, `tests/property_suite.rs` | D5 minimum property suite is live in ordinary CI |
| `M2 Scale & Survive` | C admission snapshot note | `open` | `C` + `D` | reserved in `artifact_registry.md` | Code landed in `C1`; canonical note does not exist yet |
| `M2 Scale & Survive` | C execution-topology note | `open` | `C` + `D` | reserved in `artifact_registry.md` | WAL/slab split is live; note is still missing |
| `M2 Scale & Survive` | C overload-limit matrix | `open` | `C` + `D` | reserved in `artifact_registry.md` | Limits and tests exist; operator-facing matrix does not |
| `M2 Scale & Survive` | C panic/poison disposition note | `open` | `C` + `D` | reserved in `artifact_registry.md` | C6 closed code paths; evidence note is still missing |
| `M2 Scale & Survive` | C overload-limit implementation | `done` | `C` | code + tests under `src/network/server.rs` | Listener and post-auth bounds are live |
| `M2 Scale & Survive` | C execution-topology benchmark harness | `done` | `C` | `benches/io_topology.rs` | Shared vs split modes both compile |
| `M2 Scale & Survive` | C benchmark numbers on real hardware | `open` | `C` + `D` | reserved in `artifact_registry.md` | Sandbox compile only, no authoritative numbers |
| `M2 Scale & Survive` | D critical-path metric ownership | `done` | `D` | `plans/impl/workstream_D_metric_catalog.md` | B/WAL, migration-proxy, and `io_uring` rows are live |
| `M2 Scale & Survive` | D trace inventory | `done` | `D` | `plans/impl/workstream_D_trace_inventory.md` | Request-entry and pending-reconcile span inventory is on disk |
| `M2 Scale & Survive` | D soak spec | `done` | `D` | `plans/impl/workstream_D_soak_spec.md` | D6 workload shape and promotion rule are fixed |
| `M2 Scale & Survive` | D soak report | `partial` | `D` | `plans/impl/workstream_D_soak_report.md` | Template is present; authoritative measured run is still pending |
| `M3 Customer Pilot Ready` | Audit/trace correlation inventory | `open` | `D` + `G` | reserved in `artifact_registry.md` | Depends on D2 and G |
| `M4 Regulated Pilot Authorization` | Hostile-failure evidence package | `open` | `D` | reserved in `artifact_registry.md` | Depends on D3-D6 |

## Immediate D Priorities

These are the next scorecard rows expected to move:

1. `C` admission snapshot note, execution-topology note, and overload-limit matrix from `open` to `done`
2. `C` benchmark numbers on real hardware from `open` to `done`
3. `B` write-path benchmark delta from `open` to `done`
4. `D` turn the now-live metric families into dashboard and alert inventory instead of adding more placeholder series
5. `D` execute the commodity-profile soak and replace the D6 template with measured evidence

## Rules

- A workstream is not “complete” if its promised gate artifact is still `open`.
- Bench harnesses do not count as benchmark evidence.
- Passing tests do not replace an artifact that the roadmap explicitly requires.
- If a row changes state, the artifact path must already be stable enough for someone else to find it without asking.
