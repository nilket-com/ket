# Wendelon Open Questions: Architectural Answers & Rulings

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026 (v2 — Updated after reviewing Claude & Codex)
**Context:** Rulings and opinions on the consolidated open questions from Phase 0 planning.

---

## Workstream A — Governance, ADR Closure, and Calendar Dependencies

*(No specific enumerated questions from Claude's WS A in the extraction, but the governance rules and cadence are fully endorsed and locked.)*

---

## Workstream B — Crypto Custody and Key Lifecycle (Claude)

**1. Shamir library selection.**
*Ruling:* Use `vsss-rs`. We need verifiable commitments (Feldman VSS) to ensure that a malicious or corrupted custodian share during the ceremony doesn't silently corrupt the recombined Root KEK. An unaudited constant-time library is a risk, so we will commission a targeted crypto review of `vsss-rs` as part of the FIPS lab engagement.

**2. Custodian selection for escrow.**
*Ruling:* 2 Executive (CEO/CFO), 2 Engineering (Lead Architect/VP Eng), 1 External Trustee (Outside Counsel). This prevents a purely engineering-driven rogue recombination and ensures legal/fiduciary oversight.

**3. Is BYOK required for Phase 1 or can it defer to Phase 2?**
*Ruling:* Defer to Phase 2 for full productization. If the first regulated customer asks, ship a manual BYOK setup (customer-KMS Root KEK wrapped manually) as a compensating control.

**4. Physical HSM vendor for lab testing.**
*Ruling:* Thales Luna. It is the enterprise standard, and every major CMVP lab has deep familiarity with its PKCS#11 quirks. 

**5. Cryptoperiod for Tenant KEKs.**
*Ruling:* 1 year default, with a hard per-tenant override up to 2 years. A 1-year default is a forcing function for operational hygiene.

**6. Is `retire_key` a separate operation from `rotate`, or are they combined?**
*Ruling:* Separate operations. Legal holds regularly require us to rotate to a new active key while preserving the old key indefinitely.

**7. Per-tenant Root KEK (Phase 3 of ADR 021) — when?**
*Ruling:* Post-GA. We will not complicate the Phase 1/2 key hierarchy with per-tenant roots. 

**8. DEK re-wrap on plaintext slabs still existing.**
*Ruling:* Share the iteration/sweeper scheduling code, but keep the cryptographic mutation logic strictly separated. 

**9. Memory-dump attack surface.**
*Ruling:* Out of scope for Level-1 CMVP. We will implement `ZeroizingAlignedBuf` to prevent accidental lingering in userspace, but `mprotect` hypervisor layers are out of scope.

**10. BYOK cross-KMS replication for DR.**
*Ruling:* The customer owns cross-region KEK replication. Wendelon will not orchestrate IAM/KMS replication across the customer's cloud boundaries.

**11. Primary-plan vs fallback-plan for provider shipping (new, from Codex disagreement).**
*Ruling (Updated):* Hybrid plan. AWS KMS must be production-ready for Gate 3. GCP KMS and Azure Key Vault must be ready by Gate 4. I agree with Codex that demanding all four at Phase 1 exit is too high a schedule risk.

**12. Evidence criterion for "plaintext buffers adequately zeroized" (adopted from Codex's open question).**
*Ruling:* (a) Static analysis coverage proof, (b) runtime memory-pattern tests after drop, and (c) io_uring CQE-handler tests verifying zero before reuse/unmap.

---

## Workstream C — Identity, RBAC, and Authorization (Claude)

**1. Cert-service HA topology.**
*Ruling:* 3 nodes by default. 5 nodes as a regulated-SKU deployment option.

**2. Signing HSM per-node or shared.**
*Ruling:* Shared HSM cluster by default. I am aligning with Codex here; per-node HSMs create too much key-replication complexity for v1.

**3. Is `compliance_auditor` a customer-operator role or a vendor role?**
*Ruling:* Customer-operator role. Wendelon (the vendor) should have zero default access to the customer's cluster.

**4. Break-glass scope narrowing — is five the right set?**
*Ruling:* Yes. `diagnostic`, `tenant_ops`, `cluster_ops`, `key_ops`, `full`. 

**5. Dual-auth signature aging.**
*Ruling:* 5 minutes. 

**6. What happens when an operator's cert is revoked mid-pending-proposal?**
*Ruling:* The proposal does not apply. Re-validate all signatures against current revocation state at apply time.

**7. IdP integration: JWT revocation propagation.**
*Ruling:* No proactive revocation hook for v1. The 8-hour cert lifetime is our bounded risk window.

**8. `tenant_admin` role and cross-tenant visibility.**
*Ruling (Updated):* Define the minimal grammar now. At a minimum, support `*` (all tenants) and explicit tenant allowlists. Do not defer this entirely as it blocks managed-service partner scenarios.

**9. Ed25519 vs ECDSA for proposal signatures.**
*Ruling:* ECDSA-P256 for both profiles. The consistency is worth more than the Ed25519 speed win.

**10. Should the cert-service manage tenant certs too?**
*Ruling:* Not as a general tenant CA in v1. It should manage node, operator, and short-lived query/session credentials. 

**11. Query-session cert revocation at IdP logout (new from Gemini).**
*Ruling:* Rely on the 15-minute short lifetime and Toron heartbeat-based revocation. Explicit logout-triggered revocation is best-effort.

**12. Predicate-pushdown identity propagation (new from Gemini's WS-B open question).**
*Ruling:* Compute executes under the query-session cert's identity. Wendelon enforces resource limits and data isolation based on the OID attached to the pushdown execution context.

---

## Workstream D — Audit, Evidence, and Retention (Claude)

**1. Sampling policy for range queries.**
*Ruling:* 100% for both start and completion on regulated tenants. Do not sample range-query audit in regulated posture.

**2. Is `compliance_tier` a static tenant attribute or dynamic?**
*Ruling:* Dynamic upward, controlled downward. Upgrading a tier is an audited workflow.

**3. WORM default: governance or compliance mode?**
*Ruling:* Governance mode as the default. Compliance mode opt-in per customer/tier.

**4. Signed-root key separation from mTLS node key.**
*Ruling:* Separate keys for regulated SKU. 

**5. Kafka topic partitioning.**
*Ruling:* By `node_id`. 

**6. Retention enforcement at which layer?**
*Ruling:* The local rotation sweep must verify the WORM sink's confirmed-archived status before deleting local NDJSON files.

**7. Trace-id in audit: privacy considerations.**
*Ruling:* Acceptable. The trace ID is best-effort correlation.

**8. What about non-English reason strings in break-glass?**
*Ruling:* UTF-8 encoded, no specified language. Length bounded [16, 1024].

**9. Audit archive migration during cluster rebuild.**
*Ruling:* Emit an explicit `cluster.rebuild`/fork marker and teach verification tooling to follow the chain transition.

**10. Pseudonymization for GDPR — when?**
*Ruling:* Gate 5 / regulated GA by default. 

**11. Operator-facing verification UX (adopted from Codex's open question).**
*Ruling:* CLI summary mode is enough for pilot. Dashboard integration can wait for GA.

**12. Trace-stitching for Toron-originated audits.**
*Ruling:* Correlate to the Toron-to-Wendelon request trace. Session-level correlation belongs on the Toron side.

---

## Workstream E — FIPS 140-3, Supply Chain, and Release Compliance (Codex / Claude)

**1. What exact pilot language is acceptable if CMVP is in progress but not complete?**
*Ruling:* "FIPS-ready build; CMVP validation in progress; not yet FIPS-certified." Nothing stronger.

**2. Is `rustls-aws-lc-rs` mature enough for the target regulated-pilot window?**
*Ruling:* Plan for it as the primary path, but carry an explicit fallback posture in the release plan.

**3. What minimum SLSA target do we require before regulated pilot versus regulated GA?**
*Ruling:* SLSA L2 for Pilot. SLSA L3 for GA.

**4. Which customer-facing compliance artifacts are mandatory for the first regulated pilot, and which can wait for GA?**
*Ruling:* Pilot evidence packet, SBOM, signing verification guidance, FIPS posture statement, and security architecture mapping summary. The fuller SOC 2 / PCI pack can mature toward GA.

**5. What platform set do we certify first, and which platforms are explicitly out of scope until a later validation cycle?**
*Ruling:* Linux x86_64 and aarch64. Windows, macOS, and BSD are explicitly out of scope for v1.

---

## Workstream A: Closed-Loop Healing, DR, and Operator Tooling (Gemini)

**1. Backup Storage Provider:**
*Ruling:* Backups must live separately from the live L2 tier (different bucket/account boundary).

**2. Operator Scope:**
*Ruling:* `wendelon-operator` manages the Wendelon storage tier ONLY. 

**3. Cross-Region Active Replication:**
*Ruling:* Cold restore alone is too weak for a serious regulated target. I agree with Codex here: we need asynchronous cross-region replication or a warm standby posture by the regulated pilot/GA horizon.

---

## Workstream B: Toron Compute Tier & Ecosystem Integration (Gemini)

**1. Predicate Pushdown Security:**
*Ruling:* Do not allow arbitrary WASM-style compute pushdown in the first regulated pilot. Use a constrained, versioned compute AST.

**2. Quota Visibility:**
*Ruling:* Toron surfaces storage quota as a first-class query error condition. Toron caches a snapshot for UX but must propagate structured quota violations from storage.

**3. Schema Evolution:**
*Ruling:* Toron owns schema intent; Wendelon owns durable schema state. `ALTER TABLE` must become a versioned proposal serialized through Wendelon's Raft state.

---

## Workstream C: Transport, Admission Control & Runtime Stability (Gemini / Codex)

**1. Transport Truth-Up:**
*Ruling:* Retract the GSO/io_uring QUIC claims from the documentation for the pilot. 

**2. Buffer Pooling:**
*Ruling:* Do not rewrite immediately. Measure contention after the larger runtime refactors land; shard only if the data says it matters.

**3. Connection Fairness:**
*Ruling:* Enforce coarse IP/global token buckets pre-auth, then shift to tenant fairness post-auth.

**4. Is per-device worker mapping mandatory for Gate 2? (Codex)**
*Ruling:* No. WAL/slab separation is mandatory for Gate 2. 

**5. Should QUIC admission control reject early or shed later? (Codex)**
*Ruling:* Both. Reject early on global pre-auth saturation, then use bounded authenticated queues with shedding for post-auth fairness.

**6. Which live-path aborts are truly invariant failures? (Codex)**
*Ruling:* Only memory-safety-adjacent impossible states and post-validation internal invariants. 

---

## Workstream D: Storage and Stellarator Correctness (Gemini / Codex)

**1. Leader Fencing Thresholds:**
*Ruling:* If the local persistence path fails with integrity-threatening errors, step down immediately.

**2. Client Retry Semantics:**
*Ruling:* Normalize into a stable retryable client error with `Retry-After` metadata.

**3. Orphaned Remote Data:**
*Ruling:* Mark it for asynchronous cleanup through the sweeper path. Do not synchronously block the failed write.

**4. Is local durable append a hard requirement before any quorum success can be reported? (Codex)**
*Ruling:* Yes, if the local leader is a member of the replica set. Do not allow pure-remote quorum in that case.

**5. Separate background finalization worker or existing wakeup/replay path? (Codex)**
*Ruling:* Use a dedicated background finalization/replay worker. The current wakeup path can be reused internally, but the behavior must become an explicit subsystem.

---

## Workstream E: Observability, Validation, and SLO Evidence (Gemini / Codex)

**1. Cardinality Limits:**
*Ruling:* No unbounded per-tenant labels in default exported metrics. Use sampled logs or top-N views for tenant-specific detail.

**2. Linearizability Harness:**
*Ruling:* Build the narrower in-house checker first, then run Jepsen before the regulated pilot bar.

**3. Trace Sampling:**
*Ruling:* Tail-based sampling in the collector. Keep 100% errors, writes, and finalization failures. Keep traces over the target latency threshold, and sample a low baseline of successes.

**4. What exact soak duration and load shape are required before promoting an SLO? (Codex)**
*Ruling (Updated):* Minimum of four consecutive weekly soaks, each with at least 24 hours at representative load, plus one 72-hour release-candidate soak.