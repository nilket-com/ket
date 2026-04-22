# Wendelon Execution Plan — Closing the Gap Analysis

**Author:** Claude, acting as Lead Software Architect
**Date:** 2026-04-22 (v2 — integrates peer execution plans)
**Relationship to evaluation:** This document is the execution counterpart to `claude_eval_20260422.md` (v2, which integrates peer findings from `codex_eval_20260422.md` and `gemini_eval_20260422.md`). Every gap item R-1 through R-48 in the evaluation is closed by exactly one workstream here; the mapping is at §10.
**Peer integration (v2):** Reviewed `codex_execution_20260422.md` and `gemini_execution_20260422.md`. Borrowed: Codex's Phase 0 baseline-capture discipline and CI program scorecard, Codex's explicit Stellarator write-outcome state enum, Codex's 1,000-iteration fault-injection exit criterion, Codex's Program Rules framing. From Gemini: named phase gates ("Scale & Survive", "Procurement Ready", etc.), formal red-team audit gate, and the "full teardown-and-restore using only documented runbooks" validation criterion. Scope-premise note at end of §1.
**Audience:** Engineering leadership, PM, and the review group chartered to approve regulated-pilot GA.
**Planning horizon:** 18 months to regulated-pilot GA, 24 months to multi-customer regulated GA.

---

## 1. Strategy (three load-bearing principles)

1. **Close ADR Open Questions before writing the code that depends on them.** Today every ADR is Proposed. Regulated procurement reads "Proposed" as "unstable." A 6-week Phase 0 promotes the eight ADRs to Accepted by closing their Open Questions; nothing in Phase 1 starts before that ADR is Accepted. This is cheap and it is how we stop shipping on Proposed architecture.
2. **Start the calendar-dominant items on Day 0.** CMVP lab engagement (6-9 months lead time), IANA PEN registration (4-8 weeks), KMS vendor procurement contracts (legal-gated), Tenant-KEK escrow ceremony design, and Toron co-engineering kick-off do not block on engineering readiness. They block on calendar. They start immediately and run in parallel with Phase 0.
3. **Workstreams parallelize once Phase 0 closes; merge points are structural, not cosmetic.** Eight workstreams (§3) run mostly in parallel. The dependencies between them are well-defined (§4). No workstream is a bottleneck for more than one other workstream at a time.

We do not re-architect the Unified Ring, the Identity/Locality type split, the tenant-aware schema, the envelope-encryption hierarchy, or the hash-chained audit model. The evaluation confirmed those are the hard-to-retrofit pieces and they are sound. The execution plan is integration, hardening, and governance work around that architecture.

### 1.1 Program Rules (non-negotiable)

Borrowed in shape from Codex's plan, adapted for this program's scope. These apply to every PR in every workstream:

1. **No design claim survives a code mismatch.** If a comment, ADR, or operator doc claims behavior X and the code does Y, one of them changes *in the same PR* — not in a follow-up ticket. Applies especially to transport tier naming, `io_uring` topology claims, and Stellarator fan-out concurrency.
2. **Every hot-path change ships with metrics, tests, and benchmark updates in the same PR series.** A WX-4 refactor that measurably changes allocator traffic without touching the metrics catalog is rejected at review.
3. **Correctness and crash recovery land before throughput tuning.** Within each workstream: hardening before optimization. WX-4 lands pending-intent persistence (R-32) before it lands the ring split (R-31), even though the ring split is more visible.
4. **Regulated-pilot gating is binary.** A workstream is either Gate-complete or it is not. Gate criteria are tests and artifacts, never subjective. Partial-credit estimates have no place in a gate review.
5. **No workstream claims completion without running against the CI Program Scorecard.** See §2.1 below. The scorecard is the truth.

### 1.2 Scope-premise note (vs. peer plans)

Codex's plan scopes ~10-12 weeks to a customer-facing design-partner pilot; Gemini's scopes ~11 weeks to "pilot authorization." This plan targets **regulated-industry Fortune-500 pilot → multi-customer regulated GA** over 18 months. The longer horizon is almost entirely CMVP lab calendar, `rustls-aws-lc-rs` upstream maturation, and first-customer InfoSec review — none of which engineering can compress. For the engineering-only scope, Codex's ~12 weeks and my Phase 1 (~10 months) overlap meaningfully; my additional 8 months are the compliance and GA-fit work their plans scope out of explicitly.

---

## 2. Phasing and Calendar

| Phase | Named gate | Window | Theme | Exit |
|---|---|---|---|---|
| **Phase 0** | **Foundation** | Weeks 1–6 | Open-Question closure; ADRs Proposed → Accepted; calendar-dominant items kicked off; baselines captured | All reviewed ADRs Accepted; CMVP lab contracted; IANA PEN filed; KMS vendor paper signed; baseline scorecard live |
| **Phase 1** | **Scale & Survive** | Months 2–10 | Hot-path correctness, crypto custody, identity split, first regulated-pilot enablement (Gate 1 of the eval) | First regulated pilot customer can deploy; all Gate-1 items closed |
| **Phase 2** | **Procurement Ready** | Months 9–15 | Regulated-GA enablement (Gate 2 of the eval); CMVP validation; red-team audit | Multi-customer regulated GA SKU ships; CMVP certificate issued or in final review |
| **Phase 3** | **Pilot Authorization** | Months 13–18 | GA quality bar (Gate 3 of the eval); runbook-only teardown-and-restore validation | Full ecosystem integration, SBOM/SLSA, validated SLOs |
| **Phase 4** | **Post-GA Staging** | Month 18+ | Post-GA staged work (Gate 4) | Phase-2/3 tenant topology work; advanced cipher agility |

Gate names (adapted from Gemini's plan) are what the program calls its own milestones in review and status. Phase numbers remain the cross-reference key.

Phases overlap deliberately: the customer-facing SKU ships at the end of Phase 2 with Phase-3 quality work continuing in parallel, so Phase 3 is ride-along hardening, not a release gate.

### 2.1 CI Program Scorecard (runs every PR, every night)

Borrowed from Codex. A CI artifact that the program steers by. Lives in `ci/program_scorecard.json`; rendered as a dashboard row for each workstream. Must be green before a Gate review.

| Track | Measurement | Source | Gate 1 target | Gate 2 target | Gate 3 target |
|---|---|---|---|---|---|
| Hot-path benchmarks | Raft commit p99 under concurrent slab load | criterion harness | no coupling (±10%) | (held) | (held) |
| Hot-path benchmarks | Request admission allocator bytes/req | criterion harness | ≤ 20% of baseline | ≤ 10% | ≤ 5% |
| Hot-path benchmarks | Stellarator write p99 local+remote | criterion harness | concurrent fan-out verified | 1.5× improvement | (held) |
| Chaos matrix | Scenarios passing | `tests/chaos.rs` | ≥ 20 | ≥ 30 | ≥ 30 nightly |
| Simulator ladder | Named scenarios covered | `tests/simulator_ladder.rs` | ≥ 5 | full ADR-028 matrix | full matrix |
| Fault injection | Crash iterations without stranded quorum-durable writes | custom harness | ≥ 1,000 | ≥ 10,000 | ≥ 100,000 soak |
| Metrics completeness | % of required metrics present | `ci/metric_catalog_check` | 100% WX-4 set | 100% extended | 100% |
| ADR acceptance | ADRs Accepted / reviewed | manual tracker | 6/8 | 8/8 | 8/8 |
| FIPS evidence | Symbol sweep + KATs passing | CI | sweep | sweep + KATs | sweep + KATs + CMVP cert |
| Supply chain | SBOM + signed artifact | release pipeline | SBOM | SBOM + signed | SBOM + signed + SLSA L3 |
| Runbook coverage | Paging alerts with runbooks | doc check | ≥ 60% | 100% | 100% |

The scorecard is built in Phase 0. No engineering work begins in Phase 1 until every row has a measurement pipeline wired — even if the current value is "0" or "not applicable yet." Wiring the measurement at the start means the first improvement moves the needle visibly.

### 2.2 Why 18 months and not 9

Regulated pilot readiness is gated on three items whose calendar cost we do not control:

- **CMVP lab validation** for the "Wendelon FIPS Build" — typical queue 3-6 months, validation cycle 3-6 months, total 6-12 months once lab engaged. We start Day 0; it lands in Phase 2 at the earliest.
- **`rustls-aws-lc-rs` transport path upstream stability** — ADR 026 Open Question. Depends on the upstream provider reaching the quality bar we need; we can accelerate with contribution but not force the timeline.
- **First regulated-customer security review** — a real customer's InfoSec will take 8-12 weeks of question-and-answer. We plan the pilot window around this rather than pretend it is zero.

Engineering-only timeline for the code work is closer to 9-12 months. The full 18-month horizon is the honest cost of shipping into regulated-industry buyers.

---

## 3. Workstream Charter

Eight workstreams. Each has a directly responsible tech lead, a team shape, a duration, a dependency set, and exit criteria stated as test or artifact outcomes, never as "feeling done."

### WX-1 — Crypto Custody and Lifecycle

- **Scope.** Production Root-KEK providers, key escrow, Tenant-KEK rotation operator surface, DEK re-wrap sweeper, BYOK, plaintext-buffer zeroization. Closes R-1, R-2, R-11, R-12, R-15.
- **Team.** 3 engineers (1 senior crypto, 2 systems). 1 part-time security architect. 1 part-time compliance lead for ceremony documentation.
- **Duration.** 6 months (Phase 1 critical path).
- **Dependencies.** ADR 020 Open Question "KMS backend abstraction" closed in Phase 0. Legal-signed KMS vendor contracts by end of Phase 0.
- **Work items (ordered).**
  1. Ship AWS KMS, GCP KMS, Azure Key Vault providers behind the ADR 020 `RootKekProvider` trait. Integration-test each against a live sandbox. Retire `DevInsecure` from default; retain under explicit `--features dev-insecure` flag for local development only.
  2. PKCS#11 HSM provider (Thales, Luna, Entrust via PKCS#11). Acceptance test against one physical HSM brand and one software HSM (SoftHSM2) in CI.
  3. M-of-N Shamir-split Root-KEK escrow design and implementation. Custodian-held shares in HSM-sealed envelopes; quarterly recombine-and-verify ceremony documented as `ops/key_ceremony.md`.
  4. `TenantAdmin.rotate_tenant_kek` admin RPC — operator-role gating, dual-authorization (see WX-2), audit emission, post-commit crypto-manager convergence check. Replace the in-process primitive.
  5. Background DEK re-wrap sweeper. Iterates slabs under retired Tenant-KEK versions, re-wraps Slab DEKs under the active Tenant KEK, marks retired KEK for destruction when refcount reaches zero. Configurable rate-limit and max-time-to-full-rotation SLA (default 30 days).
  6. BYOK surface — customer-KMS Root KEK with Wendelon's Tenant KEK wrapped by it. Zero-change to Slab DEK path; adds `byok = true` tenant flag and external-KMS key-id reference.
  7. Plaintext slab buffer zeroization: wrap `AlignedBuf` in `ZeroizeOnDrop` or equivalent region allocator; coordinate with ADR 009 to zero-on-munmap. Verify no code path holds a plaintext reference past drop.
  8. Maximum Tenant-KEK age enforcement; state machine rejects writes to a tenant whose active KEK age exceeds `max_tenant_kek_age` (default 365 days).
- **Exit criteria.**
  - Integration test: rotate Tenant KEK, confirm re-wrap sweeper reduces retired-KEK refcount to zero within configured SLA, destroy the retired KEK, read pre-rotation slab succeeds for the entire re-wrap window, fails cleanly after destruction.
  - `cargo build --features dev-insecure` only; default build contains no `DevInsecure` path. CI enforces.
  - Escrow recombine-and-verify ceremony passes against three independent custodians (HSMs) in a dry-run exercise.
  - `ops/key_rotation.md` is updated to reference these primitives and declares "no-escrow posture" retired.

### WX-2 — Identity, Authorization, and Governance

- **Scope.** IANA PEN + OID freeze, cert-rotation service HA design, CRL distribution, role split, dual-authorization, break-glass workflow, IdP integration. Closes R-6, R-7, R-8, R-13, R-14, R-16, R-17.
- **Team.** 2 engineers, 1 security architect (lead), 1 PM for IdP-integration partner work.
- **Duration.** 7 months (Phase 0 → Phase 1 → early Phase 2).
- **Dependencies.** IANA PEN filing (Phase 0, calendar-dominant). ADR 018 Open Questions closed in Phase 0.
- **Work items (ordered).**
  1. **Phase 0:** File IANA PEN. Freeze OID once assigned. Recorded in ADR 018 at acceptance. Until frozen, no external cert is issued.
  2. **Phase 0:** Design cert-rotation service with explicit no-deadlock property (runs as an HA standalone service; does not depend on the cluster it certifies for its own cert-issuance path). Land the design note in `plans/network/security.md` (currently 15 lines — this becomes a 150-line document).
  3. **Phase 0:** Design CRL distribution. Two options evaluated and one selected: (a) cluster-state-machine-distributed CRL or (b) separately-HA HTTP endpoint. Load-bearing constraint: 30-minute fail-closed staleness ceiling is never violated under normal conditions. Pick (a) for the regulated-pilot SKU; it removes an external HA dependency.
  4. **Phase 1:** Split `operator` into `tenant_admin`, `cluster_admin`, `backup_admin`, `compliance_auditor`. Update ADR 018 role matrix. Update `plans/ops/multi_tenancy.md` §2.3. Dispatcher enforcement layer unchanged — the change is the role enum and the required-role declarations on each RPC.
  5. **Phase 1:** Implement dual-authorization on destructive Raft proposals. Mechanism: a proposal accepted for apply only if signed by two distinct cert key-ids with the required role. Apply to `delete_tenant`, Tenant-KEK destruction, membership eviction of last-healthy quorum peer, break-glass cert issuance. State-machine validation, not dispatcher-level.
  6. **Phase 1:** Ship the cert-rotation service as a standalone binary (`wendelon-cert-service`) with its own HA topology. Deployment pattern documented.
  7. **Phase 1:** Harden break-glass issuance workflow: pre-approval by second operator, incident-ticket-id in the audit parameter field, reason string non-empty and length-bounded, time-boxed operation scope (not just lifetime).
  8. **Phase 2:** IdP integration reference — operator-cert issuance keyed on IdP group membership via the cert-rotation service. Partner integrations: Okta, Azure AD, Google Workspace, generic SAML. Documented as `ops/idp_integration.md`.
- **Exit criteria.**
  - OID frozen in ADR 018 at acceptance; all certs issued after freeze.
  - Four-role configuration exercised in test fleet; audit records correctly tagged per new role.
  - Dual-authorization gate tested: a single-signed destructive proposal is rejected by Raft apply; a two-signed proposal succeeds and emits an audit record listing both signers.
  - Cert-rotation service stands alone; failover drill shows zero cluster impact during service failover.
  - Break-glass issuance exercised end-to-end with incident-ticket-id flow.

### WX-3 — Audit Completeness and Retention

- **Scope.** Data-access audit, WORM sink, Kafka canonicalization, retention default, break-glass schema, trace correlation, audit-verification CLI. Closes R-4, R-9, R-10, R-17 (schema piece), R-18, R-19, R-43.
- **Team.** 2 engineers, 1 compliance lead (PM-shaped).
- **Duration.** 5 months.
- **Dependencies.** ADR 025 Open Questions closed in Phase 0 (break-glass schema, trace-id addendum, signed-root key decision).
- **Work items (ordered).**
  1. **Phase 0:** Close ADR 025 Open Questions. Specifically: break-glass parameter schema (invoking-cert-serial, human-name via external binding, reason string, incident-ticket-id); optional `trace_id` field on audit records; separate signing key for signed roots (regulated SKU).
  2. **Phase 1:** Extend audit event model with `data.read`, `data.write`, and `data.range_query` event types. Emitter is the RPC dispatcher; parameters are `{tenant_id, record_identity, byte_count, result_count, authority=NodeLocal}`. Default sampling: 100% writes, configurable reads (default 10%, regulated tenants 100%).
  3. **Phase 1:** Promote `audit-kafka` to default build. Canonicalize Kafka payload to include chain fields (`sequence_number`, `prev_hash`, `record_hash`, `hash_algorithm_tag`, `schema_version`). Off-box SIEM can verify the chain independently of local NDJSON.
  4. **Phase 1:** Implement `AuditSink::ObjectLockWORM` — writes to S3-Object-Lock / Azure Immutable Blob / GCS Bucket-Lock. Chain continuity across rotation boundaries is unit-tested; `prev_hash` of the first record in a new WORM object equals `record_hash` of the last record in the previous.
  5. **Phase 1:** Change retention default to 2555 days (7 years). Per-tenant retention floor config keyed on tenant compliance tier. Retention enforcement on the local NDJSON rotation path and on the WORM sink.
  6. **Phase 2:** Ship `wendelon-ctl audit verify` CLI. Replays a local chain, cross-correlates signed roots against peer nodes fetched from the `AuditChainHead.get()` RPC, produces a signed attestation document. Auditor evidence-grade.
  7. **Phase 2:** GDPR right-to-erasure workflow. Design note `ops/gdpr_erasure.md`: crypto-shredding on Tenant KEK destruction is the primary mechanism; residual `tenant_id` in audit records beyond retention window handled by audit-pseudonymization with legal review.
- **Exit criteria.**
  - Audit event schema v2 lands; `data.read`/`data.write`/`data.range_query` captured in integration tests.
  - Kafka payload carries chain fields; external-SIEM can replay and verify.
  - WORM sink integration-tested against S3 Object Lock sandbox; chain continuity verified.
  - `wendelon-ctl audit verify` produces a passing attestation against a live 3-node cluster audit stream.
  - Retention default is 7y; per-tenant tier enforced.

### WX-4 — Storage Hot Path and Stellarator Correctness

- **Scope.** `io_uring` topology split, Stellarator concurrent fan-out, mandatory pending-intent persistence, `SmState` watch, QUIC admission control, buffer pool, panic/abort inventory, control-ring wire freeze, missing metrics. Closes R-31 through R-40, R-47, R-48.
- **Team.** 4 engineers (storage + transport expertise). 1 tech lead.
- **Duration.** 7 months (Phase 1 critical path).
- **Dependencies.** None — this is internal refactoring. Parallelizable with WX-1, WX-2, WX-3.
- **Work items (ordered).**
  1. **Phase 1:** Split `io_uring` execution by durability class. One ring/thread pair for Raft WAL durability; one per physical NVMe for slab I/O. Per-ring SQ/CQ depth, submit-lag, CQE-lag metrics. Move remaining blocking file ops (`spawn_blocking` in `src/storage/file_ops.rs`) onto the correct ring. Codex-cited lines: `src/storage/io_uring.rs:3-5`, `src/node.rs:970-996`, `src/storage/file_ops.rs:141-305`. (R-31)
  2. **Phase 1:** Make pending-intent persistence mandatory for the recoverable branch of the Stellarator write path. If `pending_finalizations.upsert(...)` fails, the write fails (or the node fences) rather than continuing with quorum-durable-but-un-replayable state. Codex-cited: `src/network/data_server.rs:3402-3447`. (R-32) **Explicit write-outcome state enum** (borrowed from Codex's plan): `DataBranchPending`, `DurableQuorumReached`, `FinalizationCommitted`, `FinalizationRecoverablePending`, `FinalizationTerminalFailure`. Every transition is observable (metric + trace span) and every terminal state has defined client semantics. Ship the enum and its transition table as the first commit of the workstream; all subsequent work references it.
  3. **Phase 1:** Make Stellarator fan-out actually concurrent. Spawn remote-replication tasks before awaiting local durable append; combined quorum wait at the end. Update the comment in `src/network/data_server.rs:3328-3379` to match or deliberately keep serialization and justify in ADR 003. (R-33)
  4. **Phase 1:** Replace `SmState` full-clone in request admission with a narrow `tokio::sync::watch` on the specific admission-required fields (current max epoch, tenant status). Codex-cited: `src/network/data_server.rs:3222-3224, 3458-3460`. (R-34)
  5. **Phase 1:** QUIC admission control. Listener-level: max concurrent connections (total + per-peer-IP), max streams per connection, bounded task pool. Post-auth: per-tenant queue-length caps keyed on `tenant_id` from the OID. (R-35)
  6. **Phase 1:** Transport-tier claim reconciliation. Either implement `io_uring` + GSO on the QUIC path or retract the claim from `src/network/transport.rs` and re-benchmark. Recommendation: retract for Phase 1, target reintroduction in Phase 3 under a new ADR. (R-36)
  7. **Phase 1:** Replace the mutex-guarded global `BufPool` with sharded per-runtime-worker caches and a global overflow pool. Benchmark high-connection-count contention. (R-37)
  8. **Phase 2:** `expect(...)` / poisoned-lock inventory in live paths. Convert recoverable sites to explicit error handling; document the audited-unreachable sites that legitimately abort. (R-38)
  9. **Phase 2:** Freeze control-ring wire contracts. Model `SchemaUpdate` in durable state. Promote `ClusterStatusResponseMeta` and `TransferLeadershipRequest/ResponseMeta` to frozen wire layouts. Re-run `ops/upgrades.md` rolling-upgrade window against the final layout. (R-39)
  10. **Phase 2:** Replace the `tests/architecture_test.rs` "forbidden word count" with structural assertions: one `MembershipConfig`, one epoch source, no second listener carrying routing. Keep the word-count as a secondary regression guard, not the primary invariant. (R-40)
  11. **Phase 1:** Ship missing metrics: pending-finalization backlog and replay, replica fan-out latency, quorum acquisition, `io_uring` SQ/CQ depth, WAL append + fsync/fdatasync latency, active connection/stream counts, rejected stream count, lock-contention indicators on major shared structures. (R-47)
  12. **Phase 2:** Move hardcoded policy values (BufPool shape, `DEFAULT_RING_DEPTH`, architecture-test threshold) to configuration with documented defaults. (R-48)
- **Exit criteria.**
  - Benchmark: Raft commit-latency p99 decoupled from concurrent slab-write load (split-ring goal). Evidence from chaos-harness soak.
  - **1,000 injected crash-iteration fault harness** (borrowed from Codex) completes without a single stranded quorum-durable write. Iterations cover: crash after data-branch quorum pre-finalization, pending-intent-store failure, remote replica stall/timeout, restart replay of queued finalizations, duplicate-client-retry during pending finalization. The 1k-iteration result is a gate artifact, not a spot-check.
  - Chaos scenario: kill a node mid-Stellarator-write at the local-fsync-completed / pending-intent-persist-failed boundary; on restart, the write either finalizes correctly or is cleanly rolled back. No stranded quorum-durable data.
  - Benchmark: `SmState` clone cost replaced; request admission allocator churn measurably reduced per CI scorecard targets.
  - QUIC admission control: per-peer-IP connection-flood chaos test caps at the configured limit; other tenants' p99 unchanged. DoS simulation from a single tenant does not move any other tenant's p99 beyond 5%.
  - Write-outcome state enum is authoritative: every terminal `FinalizationTerminalFailure` maps to a retry-safe client error; every `FinalizationRecoverablePending` is retryable and replay-deterministic; metrics for each transition published.
  - Architecture test passes its strengthened assertions; no regression path for membership-graph duplication or secondary-listener drift.

### WX-5 — Disaster Recovery and Backup

- **Scope.** DR plan with measured RTO/RPO, PITR, cross-region replication, cluster-restore choreography, tested drill, backup encryption. Closes R-5, R-25.
- **Team.** 2 engineers + 1 SRE/ops lead.
- **Duration.** 6 months.
- **Dependencies.** WX-1 (Tenant-KEK state feeds backup encryption). WX-4 (control-ring wire freeze before backup format freezes).
- **Work items (ordered).**
  1. **Phase 1:** Author `ops/dr_plan.md`. Quantitative RTO and RPO by SKU: regulated-SKU RTO 4h warm-standby, 24h cold-restore; RPO 15m L1, 1h L2. Define failover decision tree, integrity verification post-restore, legal-hold freeze procedure.
  2. **Phase 1:** Cluster-level consistent-cut backup. Raft-coordinated snapshot across all nodes at a single log index; L2 pointer stabilization; manifest records the stable index.
  3. **Phase 1:** Cross-region replication of L2. Two options: (a) provider-native cross-region replication with a Wendelon-aware manifest that records the replication policy; (b) Wendelon-managed replicator that reads from the home bucket and writes to the DR bucket with tenant-aware prioritization. Ship (a) as the default for Phase 1; architect (b) in Phase 2 for residency-aware replication.
  4. **Phase 1:** Backup encryption design. Backup manifest carries wrapped Tenant KEKs (already encrypted by Root KEK); local redb + WAL + vote artifacts encrypted at backup time under a backup-specific DEK wrapped by the Root KEK. Restore requires Root-KEK unwrap access.
  5. **Phase 2:** Point-in-Time Recovery (PITR). Combines Raft log retention + L2 immutability + time-travel query support (ADR 013, not in eval scope but integration with DR is here). Target: restore tenant X state as of timestamp T within RTO.
  6. **Phase 2:** Tested DR drill. Full cluster loss → restore in DR region → customer traffic redirected. Evidence: RTO/RPO measured, tenant parity verified, audit chain continuity confirmed.
  7. **Phase 2:** Reference Terraform/Pulumi modules for cross-region replication. Ships with the regulated SKU.
- **Exit criteria.**
  - `ops/dr_plan.md` is 15-30 pages, signed off by legal and compliance.
  - DR drill measured: RTO within 4h, RPO within 15m for L1, audit chain continuous, zero data loss for Raft-committed writes.
  - PITR test: restore a tenant to a timestamp 48h in the past, read-verify against a held-out snapshot.
  - Cross-region replication validated end-to-end.

### WX-6 — Day-2 Ops and Ecosystem Integration

- **Scope.** Synchronous node replacement (K8s Operator / sidecar), lazy-migration QoS, Toron integration runbook, ELT connectors, runbooks, hardening guide. Closes R-30, R-41, R-42, R-44, R-45.
- **Team.** 2 engineers + 1 PM + 1 part-time Toron-side tech lead. Ecosystem-partner work is the PM-heavy piece.
- **Duration.** 8 months (runs late-Phase-1 through Phase 3).
- **Dependencies.** WX-2 (cert-rotation service model for Operator-issued identities). WX-4 (control-ring wire freeze).
- **Work items (ordered).**
  1. **Phase 1:** First-party Kubernetes Operator (`wendelon-operator`). Watches CRDs: `WendelonCluster`, `WendelonNode`, `WendelonTenant`. Performs synchronous node replacement on eviction: monitors Raft membership events, provisions a replacement pod, generates cert via cert-rotation service, joins cluster, confirms `/ready`. Zero cloud-SDK dependency in the core; operator is a separate binary and Helm chart.
  2. **Phase 1:** Companion sidecar for non-K8s deployments. Same replacement logic against a customer-provided node-provisioning hook (shell script, Ansible, Terraform apply).
  3. **Phase 1:** Lazy-migration QoS. Rate-limit migration reads per source-range and per-proxy-node. Per-tenant backpressure on migration-read proxying. Chaos scenario added to `tests/chaos.rs`.
  4. **Phase 1:** Runbooks for every paging alert: `RaftNoLeader`, `GroupCommitWindowHigh`, `AuditMaterializationLagRising`, `AuditSinkFailing` first; then `L1ReadLatencyHigh`, `L2RangeReadLatencyHigh`, `L2UploadLatencyHigh`, `TenantKekCacheThrashing`, `QuicHandshakeFailureSpike`. `plans/ops/runbooks/<alert>.md` structure.
  5. **Phase 2:** Toron integration. Formally defined storage/compute boundary: Arrow-over-QUIC contract, tenant-quota visibility from SQL clients, observability stitching (trace context propagated from Toron into Wendelon spans), mTLS-OID ↔ SQL session authorization mapping. Document as `ops/toron_integration.md` + `ops/toron_identity.md`. End-to-end integration test fleet (Toron + Wendelon) added to release gates.
  6. **Phase 2:** ELT connector reference implementations. Fivetran destination, Airbyte destination, dbt adapter. Either first-party or vendor-co-engineered. Ships with sample pipelines.
  7. **Phase 2:** Hardening guide `ops/hardening.md`. Operator OS baseline, kernel-version floors (tied to ADR 026 Open Question), network-policy minimum, SELinux/AppArmor profiles, secrets-management integration patterns.
- **Exit criteria.**
  - K8s Operator replaces an evicted node within configured SLA (default 5 min). Durability-degraded window measured and reported.
  - Toron integration test fleet passes end-to-end: SQL query → Toron → Wendelon → Arrow result → SQL client, with audit records correctly stitched.
  - At least two ELT connectors demonstrated on real ingestion pipelines.
  - Every paging alert has a runbook.

### WX-7 — Test, Chaos, and SLO Validation

- **Scope.** Chaos matrix expansion, simulator ladder, linearizability checking, SLO validation soak, FIPS test strengthening. Closes R-23, R-46.
- **Team.** 2 engineers (reliability / testing focus) + 1 rotating engineer from each of WX-1, WX-4, WX-6 for scenario coverage.
- **Duration.** 6 months (runs Phase 1 into Phase 2).
- **Dependencies.** WX-4 (fixed hot paths are what we soak); WX-5 (DR scenarios in the chaos matrix); WX-1 (rotation scenarios).
- **Work items (ordered).**
  1. **Phase 1:** Expand `tests/chaos.rs` from 3 scenarios to a ~30-scenario matrix covering nemesis combinations (partition + disk fill, partition + clock skew, leader flap + rotation, node eviction + in-flight write, packet drop + replication lag, topology change + analytical query). Named-seed reproducibility preserved.
  2. **Phase 1:** Expand simulator ladder from `coverage_ladder` (one scenario) to the full matrix referenced in ADR 028. Upgrade-window, migration, lease churn, transport degradation, crypto-profile-roll each become named scenarios.
  3. **Phase 2:** Linearizability / Jepsen-style coverage. Either integrate with Jepsen directly or implement an equivalent in-house checker. Target: read/write linearizability under leader failover, partition, and Stellarator partial-failure scenarios.
  4. **Phase 2:** SLO validation soak. Weekly coverage-ladder runs that hit each SLO target for ≥4 consecutive weeks. Downgrade `TARGET (unvalidated)` → `COMMITTED` in `ops/observability.md` only after this bar is met.
  5. **Phase 1:** FIPS validation beyond the symbol sweep. Known-answer tests (KATs) for each primitive used in the FIPS build. Conditional-test exposure as metrics. Self-test exercised at every bootstrap; a failure aborts startup.
- **Exit criteria.**
  - Chaos matrix runs nightly; coverage report on PR merges.
  - Simulator ladder covers every named scenario in ADR 028.
  - At least one identified correctness issue found and fixed by linearizability harness before Phase 2 close.
  - SLO targets move from TARGET to COMMITTED.

### WX-8 — FIPS Certification and Compliance Delivery

- **Scope.** CMVP lab engagement, FIPS transport-path expansion, Security Policy authoring, customer-facing FIPS evidence. Closes R-3, R-22, R-24, R-26, R-27, R-29.
- **Team.** 1 senior engineer (FIPS-build owner), 1 compliance lead (full-time), part-time legal. External: CMVP-accredited lab contracted.
- **Duration.** 12 months (Phase 0 kickoff through Phase 2 certificate).
- **Dependencies.** Upstream `aws-lc-rs` CMVP status (external). `rustls-aws-lc-rs` stability (external). ADR 026 Open Questions closed in Phase 0.
- **Work items (ordered).**
  1. **Phase 0:** Lab selection and contracting (atsec / Leidos / Acumen). Engagement kicks off; calendar-dominant.
  2. **Phase 0:** Close ADR 026 Open Questions: aws-lc-rs CMVP state at pinned version, transport-TLS plan, entropy-source platform coverage, lab selection.
  3. **Phase 1:** Migrate transport to `rustls-aws-lc-rs` under the FIPS build profile. Extend the FIPS module boundary to include transport. Re-run ADR 019/026 acceptance.
  4. **Phase 1:** Security Policy document (lab co-authored). Platform coverage list, testable boundary definition, operational test plan (self-test, KATs, conditional tests). Entropy-source statement.
  5. **Phase 1:** SBOM (SPDX format, per-build), artifact signing (cosign/sigstore), SLSA Level 3 target. Supply-chain chain-of-trust documented.
  6. **Phase 2:** CMVP submission package. Lab validation cycle.
  7. **Phase 2:** CMVP certificate issued. Record number in ADR 026 at final acceptance.
  8. **Throughout Phases 0-2:** ADRs Proposed → Accepted. Each ADR's acceptance gate is independently reviewed; no ADR ships Accepted while its Open Questions remain.
- **Exit criteria.**
  - All reviewed ADRs are Accepted by Phase 2 close (R-29).
  - CMVP certificate number recorded in ADR 026 (R-3).
  - SBOM + signed artifacts + SLSA L3 attestation on every release (R-22).
  - FIPS-mode-only metrics (`fips_module_self_test_total`, `fips_module_version_info`) exposed and documented.
  - Customer-facing compliance pack: SOC 2 Type 2 scoping document, PCI-DSS applicability statement, HIPAA covered-entity mapping, NIST SP 800-53 Rev 5 control mapping.

---

## 4. Cross-Workstream Dependency DAG

```
 Phase 0 (calendar starts, ADR closures)
   │
   ├─ WX-2 Identity  ──┐
   ├─ WX-8 FIPS/Compliance ──┐  (parallel; lab engagement calendar-dominant)
   ├─ (IANA PEN filed)       │
   ├─ (KMS vendor contract)  │
   └─ (ADR Open Questions) ──┤
                             │
 Phase 1 (engineering)       │
   │                         │
   ├─ WX-1 Crypto  ─────────┐│  (needs KMS contract, ADR 020 closed)
   ├─ WX-2 Identity ────────┤│  (OID frozen, cert-rotation design)
   ├─ WX-3 Audit ───────────┤│
   ├─ WX-4 Hot-path ────────┤│  (internal refactor; minimal external deps)
   ├─ WX-5 DR ──────────────┤│  (uses WX-1 Tenant KEKs, WX-4 wire freeze)
   ├─ WX-6 Day-2 ───────────┤│  (uses WX-2 cert-rotation for Operator identity)
   ├─ WX-7 Test ────────────┤│  (rides along on WX-1, WX-4, WX-5 coverage)
   └─ WX-8 FIPS ────────────┤│  (transport migration to aws-lc-rs)
                             │
 Phase 2 (enable GA SKU)     │
   │                         │
   ├─ First regulated customer pilot (WX-1 + WX-2 + WX-3 + WX-5 + WX-6 + WX-8 all at acceptance bar)
   ├─ CMVP validation cycle completes (WX-8)
   ├─ Toron integration GA (WX-6)
   └─ SLOs → COMMITTED (WX-7)
                             │
 Phase 3 (quality bar)       │
   └─ Remaining Gate 3 items from eval
```

Key serialization points:
- **WX-2 OID freeze (Phase 0) blocks WX-1 KMS cert issuance, WX-3 audit actor binding, WX-6 Operator cert issuance.** This is why WX-2 starts earliest and has the tightest Phase 0 exit.
- **WX-4 control-ring wire freeze (Phase 2) blocks WX-5 backup format freeze.** We can start backup work in Phase 1 against the unfrozen layout but must ratify before GA.
- **WX-8 FIPS lab calendar is the longest single dependency.** All FIPS-SKU work shadows this; nothing FIPS-gated ships before WX-8 Phase 2 exit.

---

## 5. Resourcing

| Workstream | Engineers | Non-eng | Total FTE |
|---|---|---|---|
| WX-1 Crypto | 3 | 1 security architect (50%), 1 compliance (25%) | 3.75 |
| WX-2 Identity | 2 | 1 security architect (50%), 1 PM (50%) | 3.00 |
| WX-3 Audit | 2 | 1 compliance (50%) | 2.50 |
| WX-4 Hot-path | 4 | — | 4.00 |
| WX-5 DR | 2 | 1 SRE lead | 3.00 |
| WX-6 Day-2 | 2 | 1 PM, 1 Toron-side TL (25%) | 3.25 |
| WX-7 Test | 2 | + rotating 0.5 from WX-1/4/6 | 3.50 |
| WX-8 FIPS | 1 | 1 compliance lead, part-time legal (25%), external lab | 2.25 |
| **Totals** | **18 eng** | **~7 FTE non-eng** | **~25 FTE** |

Scope is a real 25-FTE, 18-month investment. Attempting this with materially less will either drop a workstream (most likely WX-6 or WX-7) or slip the calendar by ~6 months per halved team.

---

## 6. Exit Criteria per Phase

### Phase 0 exit (Week 6)

- All reviewed ADRs (018, 019, 020, 021, 025, 026, 027, 029) have their Open Questions closed. Each moves Proposed → Accepted.
- IANA PEN filed; OID assignment pending.
- CMVP lab contracted; kickoff scheduled.
- KMS vendor contracts signed for AWS + GCP + Azure.
- `ops/dr_plan.md` v1 drafted (final during Phase 1).
- `plans/network/security.md` expanded from 15 lines to full cert-rotation and CRL-distribution design.

### Phase 0 exit artifacts (in addition to above)

- CI Program Scorecard wired and reporting; all rows present, measurement pipelines live.
- Benchmark baselines captured for Raft commit latency (isolated and under load), write-path latency through `data_server`, read-path admission latency. Published to `ci/baselines/20260501.json` (or the actual date).
- Fault-injection harness skeleton in place for: crash after data quorum / pre-finalization, local pending-intent persistence failure, remote replica stall, transport overload.

### Phase 1 exit (Month 10)

- Crypto: production KMS providers + escrow + rotation RPC + re-wrap sweeper all ship. `DevInsecure` moved behind `dev-insecure` feature flag.
- Identity: four-role model, dual-authorization, OID frozen, cert-rotation service standalone.
- Audit: data-access events, Kafka canonical payload, WORM sink, 7y retention default.
- Hot-path: split `io_uring`, concurrent Stellarator fan-out, mandatory pending-intent, narrow `SmState` watch, QUIC admission control.
- DR: consistent-cut cluster backup, cross-region L2 replication, backup encryption, measured RTO/RPO on a drill.
- Day-2: K8s Operator ships; synchronous node replacement within SLA.
- Test: 30-scenario chaos matrix nightly; full simulator ladder.
- FIPS: `rustls-aws-lc-rs` transport migration complete; Security Policy draft submitted.

**Gate: first regulated customer pilot deploys with full Gate 1 items closed.**

### Phase 2 exit (Month 15)

- CMVP certificate issued; recorded in ADR 026.
- SBOM + SLSA L3 on every release.
- Toron integration GA.
- Linearizability coverage demonstrates at least one fixed correctness issue.
- SLOs promoted to COMMITTED.
- PITR shipped and tested.
- ELT connector references in market.
- All ADRs Accepted (not merely most).

**Gate: regulated GA SKU ships to multiple customers.**

**Gate 2 validation additions (borrowed from Gemini):**
- **Third-party red-team audit** of OID authorization logic, key-custody path, cert-rotation service, and dual-authorization enforcement. Engagement scheduled at Phase 2 mid-point; findings must be closed or explicitly accepted with compensating controls before gate exit.
- Pre-customer InfoSec-review dry run with a friendly customer's security team completed.

### Phase 3 exit (Month 18)

- Runbooks complete.
- Hardening guide published.
- Gate 3 items from the eval closed.
- Post-GA staged work (Phase-1/2/3 tenant topology) has a committed schedule.

**Gate 3 validation (borrowed from Gemini):**
- **Full end-to-end teardown and restore of a test cluster using only the documented runbooks** — no tribal knowledge, no "ask the architect." The team performing the exercise has never seen the cluster; they have only `plans/ops/`. Any deviation from runbook success is a runbook bug and fixed before gate exit.

---

## 7. Execution Risk Register

Risks to *the execution plan itself*, distinct from the system-level risks in the eval.

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| E-1 | CMVP lab queue extends past 9 months from engagement | Medium | Slips regulated-SKU GA by 3-6 months | Contract in Phase 0; run lab engagement in parallel with engineering, not serial. Accept "FIPS-ready, certification in progress" as pilot-phase posture per ADR 026. |
| E-2 | `rustls-aws-lc-rs` upstream blocks on a quality issue | Medium | Blocks FIPS transport path | Prepare contribution budget; allocate 0.5 FTE to upstream collaboration. Fallback: document transport outside module boundary for pilot, target for Phase 2. |
| E-3 | First customer InfoSec review uncovers a design issue we missed | High | Rework cost in Phase 1 or 2 | Engage a friendly customer's InfoSec team at Phase 0 exit for a pre-review. Budget 15% of Phase 1 engineering capacity for review-driven changes. |
| E-4 | WX-4 hot-path refactors introduce regressions | High | Schedule slip + quality concerns | Behind feature flags where possible; chaos harness catches regressions; gradual rollout across test fleet before enabling default. |
| E-5 | Toron team's roadmap diverges from Wendelon's | Medium | WX-6 slips or under-delivers | Cross-project co-engineering agreement; Toron-side TL dedicated at 25%; joint weekly review. |
| E-6 | Staffing ramp too slow to hit 25 FTE by Phase 1 start | Medium | All workstream durations scale | Phase 0 staffing plan; contractor augmentation acceptable for WX-6 ELT connectors and WX-7 test infrastructure. |
| E-7 | KMS vendor contract negotiations drag past Phase 0 | Low | Blocks WX-1 kickoff | Start legal engagement in Week 1 of Phase 0; parallel negotiations with all three vendors. |
| E-8 | ADR acceptance review stalls on cross-ADR consistency concerns | Medium | Phase 0 slips | Review group chartered in Week 1; one ADR per week accepted on a fixed cadence, architect signs off. |
| E-9 | Observability coverage additions introduce cardinality regressions | Low | Metrics-backend cost spike | Cardinality discipline test in CI; refuse to merge metrics with unbounded labels. |
| E-10 | Dual-authorization on destructive ops creates operational deadlock during incidents | Medium | Incident MTTR inflation | Break-glass path retains single-authorization with audit tagging; dual-auth is the default path, not the only path. |
| E-11 | Red-team audit (Gate 2) finds a structural authorization issue | Medium | Re-work across WX-2 / WX-3 | Engage red-team by Phase 2 mid-point, not at Phase 2 exit, so findings fit inside the remaining window. Budget 10% of Phase 2 engineering for audit-driven changes. |
| E-12 | "Runbook-only teardown and restore" (Gate 3) exposes tribal-knowledge dependencies | High | Gate 3 slips; runbooks need rewriting | Dogfood runbooks in every chaos drill from Phase 1 onward. Runbook authors are not the same engineers who execute the validation. |

---

## 8. Quarter-by-Quarter Roll-up

**Q2 2026 (Weeks 1-13).** Phase 0 completes in Weeks 1-6. Phase 1 starts Week 7. WX-2 OID freeze (Week 4). WX-1 KMS providers in progress. WX-4 io_uring split design merged, implementation begins. WX-8 lab kickoff. First ADRs Accepted.

**Q3 2026 (Weeks 14-26).** WX-1 KMS providers ship. WX-4 Stellarator concurrent fan-out + mandatory pending-intent merged. WX-2 dual-auth lands. WX-5 consistent-cut backup. WX-7 chaos matrix expanded. Rusttls-aws-lc-rs migration in progress.

**Q4 2026 (Weeks 27-39).** WX-6 K8s Operator ships. WX-3 data-access audit + WORM sink land. WX-5 cross-region replication. WX-7 simulator ladder complete. WX-8 Security Policy submitted to lab. First regulated pilot customer onboarded (Phase 1 exit milestone at end of quarter).

**Q1 2027 (Weeks 40-52).** Phase 2 work. Toron integration GA (WX-6). PITR ships (WX-5). Linearizability coverage (WX-7). CMVP validation cycle in progress. All ADRs Accepted.

**Q2 2027 (Weeks 53-65).** Phase 2 exit target. CMVP certificate issued. Regulated GA SKU ships. SLOs COMMITTED. ELT connectors in market.

**Q3 2027 (Weeks 66-78).** Phase 3 runs to quality bar. Hardening guide, runbook completion, Gate 3 items. Post-GA tenant-topology staging begins.

---

## 9. Governance Cadence

- **Weekly architect review.** Every Friday. One hour. Every workstream TL reports: progress, blockers, exit-criteria evidence. No status-theater; if there is nothing to report, the TL doesn't attend.
- **Monthly ADR review.** One ADR per month is the target acceptance rate. Review group: architect lead + security architect + compliance lead + one tenured engineer rotating from any workstream.
- **Quarterly customer-review-board.** Pilot and prospective-pilot customers attend a quarterly review of the roadmap. Their input shapes priority within Phase 2 and Phase 3 scope, not Phase 1 critical path.
- **Ad-hoc incident response.** WX-1 + WX-2 TLs are first-call for any security-relevant incident during execution. IR-plan authoring (part of WX-6 runbooks) lands by end of Phase 1.

---

## 10. Gap-Item → Workstream Mapping

Every eval risk item R-1..R-48 is closed by exactly one workstream. Traceability table for the acceptance review:

| R-# | Workstream | Phase | R-# | Workstream | Phase |
|---|---|---|---|---|---|
| R-1 | WX-1 | 1 | R-25 | WX-5 | 2 |
| R-2 | WX-1 | 1 | R-26 | WX-7 (observability polish) | 3 |
| R-3 | WX-8 | 2 | R-27 | WX-3 (tighter cadence option) | 3 |
| R-4 | WX-3 | 1 | R-28 | deferred (post-GA) | 4 |
| R-5 | WX-5 | 1 | R-29 | WX-8 (ADR acceptance) | 0-2 |
| R-6 | WX-2 | 1 | R-30 | WX-6 (runbooks) | 2 |
| R-7 | WX-2 | 1 | R-31 | WX-4 | 1 |
| R-8 | WX-2 | 0 | R-32 | WX-4 | 1 |
| R-9 | WX-3 | 1 | R-33 | WX-4 | 1 |
| R-10 | WX-3 | 1 | R-34 | WX-4 | 1 |
| R-11 | WX-1 | 1 | R-35 | WX-4 | 1 |
| R-12 | WX-1 | 1 | R-36 | WX-4 (retract or implement) | 1 |
| R-13 | WX-2 | 0-1 | R-37 | WX-4 | 1 |
| R-14 | WX-2 | 0-1 | R-38 | WX-4 | 2 |
| R-15 | WX-1 | 1 | R-39 | WX-4 | 2 |
| R-16 | WX-2 | 1 | R-40 | WX-4 | 2 |
| R-17 | WX-3 (schema) + WX-2 (workflow) | 0-1 | R-41 | WX-6 | 1 |
| R-18 | WX-3 | 1 | R-42 | WX-6 | 1 |
| R-19 | WX-3 | 2 | R-43 | WX-3 | 1 |
| R-20 | documented in WX-6 SLA | 1 | R-44 | WX-6 | 2 |
| R-21 | deferred (post-GA) | 4 | R-45 | WX-6 | 2 |
| R-22 | WX-8 | 1-2 | R-46 | WX-7 | 1-2 |
| R-23 | WX-7 | 2 | R-47 | WX-4 | 1 |
| R-24 | WX-8 (documented) | 2 | R-48 | WX-4 | 2 |

---

## 11. What This Plan Does Not Commit To

Honesty about scope limits.

- **No re-architecture.** The Unified Ring stays. Identity/Locality stays. Envelope hierarchy stays. Tenant-aware schema stays. Audit event model stays. The eval confirmed these are sound; the execution plan is hardening around them.
- **No on-prem HSM hardware sourcing.** WX-1 supports PKCS#11; customers procure their own HSM.
- **No multi-region active-active consensus.** Raft remains single-region; cross-region is replication, not consensus. Active-active is a multi-quarter architectural change outside this plan.
- **No SQL query language in Wendelon itself.** Toron owns SQL; WX-6 integrates the two. A Wendelon-native SQL surface is not in scope.
- **No Phase-3 per-tenant Root KEK isolation at pilot.** Committed to Phase 4 (post-GA). Customers requiring this run a single-tenant cluster as a compensating control.
- **No legacy v1-on-disk format compatibility.** Greenfield posture preserved. Format breaks happen before GA or behind a documented migration; after GA, format is stable.

---

## 12. Go / No-Go Recommendations

Explicit, gate-aligned (framing borrowed from Codex):

- **Internal pilot (single tenant, colocated operators, no customer data):** may proceed at **end of Phase 0** *if and only if* the Program Scorecard is green on baseline capture, the OID is frozen, and operators acknowledge `dev-insecure` custody in writing. No external reliability commitments.
- **Customer-facing design-partner pilot (non-regulated):** may proceed at **end of Phase 1 ("Scale & Survive")** *if and only if* all Gate-1 workstream exits are green: Stellarator correctness, `io_uring` split, admission control, production KMS path, K8s Operator, first runbooks, chaos matrix at ≥20 scenarios, 1,000-iteration fault injection clean.
- **Regulated-industry Fortune-500 pilot:** may proceed at **end of Phase 2 ("Procurement Ready")** *if and only if* Gate-1 items plus: CMVP certificate issued (or lab-final-review with a documented issuance date), red-team audit closed, WORM audit sink live, data-access audit shipping, 7-year retention enforced, dual-authorization on destructive ops. No earlier.
- **Multi-customer regulated GA:** may proceed at **end of Phase 3 ("Pilot Authorization")** *if and only if* the runbook-only teardown-and-restore validation passes on a team that has never touched the cluster.

Do **not** advertise five-nines availability, NVMe-saturating storage, or hardened bifurcated-write recovery until Phase 3 exit and backed by fresh scorecard evidence. Until then, the honest posture is "hardened design, validation in progress."

## 13. Closing

The gap analysis identified 48 findings across design, code, and operations. Nine of those (R-1..R-5, R-31, R-32, R-41, R-44) are CRITICAL and gate regulated-pilot deployment. The execution plan closes all nine in Phase 1 and treats Phases 2-3 as the path to multi-customer regulated GA.

The total investment is ~25 FTE over 18 months plus external CMVP lab fees and customer-facing compliance documentation. The shape of the investment is bounded: no workstream requires re-architecting a hard-to-retrofit component, every workstream has testable exit criteria tracked on a CI scorecard, and the peer-review convergence (three lenses landing in the same 48-55% pilot-readiness band; three execution plans broadly agreeing on the engineering critical path) gives a shared baseline from which progress is measurable.

The plan is deliverable, but only if Phase 0 is treated as non-negotiable. ADRs ship Accepted before their implementations begin. Calendar-dominant items (CMVP lab, IANA PEN, KMS contracts) start in Week 1. Program Scorecard is wired before the first Phase-1 commit. If Phase 0 slips, every subsequent phase slips with it; there is no recovery path that does not cost the calendar dollar-for-dollar.

Everything else is engineering.
