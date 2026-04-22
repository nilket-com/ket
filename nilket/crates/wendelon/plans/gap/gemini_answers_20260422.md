# Wendelon Open Questions: Architectural Answers & Rulings

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026 (v5 — Final Polish After Issues Clean-Up)
**Context:** Rulings and opinions on the consolidated open questions from Phase 0 planning, incorporating final concessions and cross-council convergences.

---

## Workstream A — Governance, ADR Closure, and Calendar Dependencies

*(Governance rules and cadence are fully endorsed and locked.)*

---

## Workstream B — Crypto Custody and Key Lifecycle (Claude)

**1. Shamir library selection.**
*Ruling:* Use `vsss-rs`. We need verifiable commitments (Feldman VSS). We will commission a targeted crypto review of `vsss-rs` as part of the FIPS lab engagement.

**2. Custodian selection for escrow.**
*Ruling:* 2 Executive (CEO/CFO), 2 Engineering (Lead Architect/VP Eng), 1 External Trustee (Outside Counsel). 

**3. Is BYOK required for Phase 1 or can it defer to Phase 2?**
*Ruling (Locked to Codex):* Productized BYOK is a Gate 4 requirement for any pilot marketed as "regulated-ready" and multi-tenant. If we miss this bar, the pilot must be explicitly scoped to Wendelon-managed keys. Manual BYOK is a compensating control only for pilots explicitly lacking broader regulated crypto-custody parity.

**4. Physical HSM vendor for lab testing.**
*Ruling:* Thales Luna.

**5. Cryptoperiod for Tenant KEKs.**
*Ruling:* 1 year default, with a hard per-tenant override up to 2 years. 

**6. Is `retire_key` a separate operation from `rotate`, or are they combined?**
*Ruling:* Separate operations. 

**7. Per-tenant Root KEK (Phase 3 of ADR 021) — when?**
*Ruling:* Post-GA. 

**8. DEK re-wrap on plaintext slabs still existing.**
*Ruling:* Share the iteration/sweeper scheduling code, but keep the cryptographic mutation logic strictly separated. 

**9. Memory-dump attack surface.**
*Ruling:* Out of scope for Level-1 CMVP. 

**10. BYOK cross-KMS replication for DR.**
*Ruling:* The customer owns cross-region KEK replication. 

**11. Primary-plan vs fallback-plan for provider shipping.**
*Ruling (Locked to Codex):* Conservative hybrid plan. AWS KMS must be production-ready for Gate 3. GCP KMS and Azure Key Vault must be ready by Gate 4. PKCS#11 trails to Gate 5 unless explicitly requested by a named pilot customer.

**12. Evidence criterion for "plaintext buffers adequately zeroized".**
*Ruling:* (a) Static analysis coverage proof, (b) deterministic runtime memory-pattern tests in a controlled harness after drop/release, and (c) io_uring CQE-handler tests verifying zero before reuse/unmap. 

---

## Workstream C — Identity, RBAC, and Authorization (Claude)

**1. Cert-service HA topology.**
*Ruling:* 3 nodes by default. 5 nodes as a regulated-SKU deployment option.

**2. Signing HSM per-node or shared.**
*Ruling (Procurement-Gated Decision):* Shared HSM cluster is the default for regulated v1 deployments to minimize complexity. Per-node HSMs are approved ONLY if the Phase 1 Month 2 HSM procurement budget explicitly permits 3+ Luna HSMs per region. For non-regulated, low-cost deployments (e.g., $5/mo VPS), a software-only (no-HSM) mode is officially supported, storing the signing key on local disk protected by OS permissions. This is critical because a bare metal server with an HSM is 10 to 100 times the cost of a basic VPS, and we must not price out developer adoption.

**3. Is `compliance_auditor` a customer-operator role or a vendor role?**
*Ruling:* Customer-operator role. 

**4. Break-glass scope narrowing — is five the right set?**
*Ruling:* Yes. `diagnostic`, `tenant_ops`, `cluster_ops`, `key_ops`, `full`. 

**5. Dual-auth signature aging.**
*Ruling:* 5 minutes. 

**6. What happens when an operator's cert is revoked mid-pending-proposal?**
*Ruling:* The proposal does not apply. Re-validate all signatures against current revocation state at apply time.

**7. IdP integration: JWT revocation propagation.**
*Ruling:* No proactive revocation hook for v1. 

**8. `tenant_admin` role and cross-tenant visibility.**
*Ruling (Concession to 2-1 Majority):* Define the minimal grammar now. At a minimum, support `*` (all tenants) and explicit tenant allowlists. 

**9. Ed25519 vs ECDSA for proposal signatures.**
*Ruling:* ECDSA-P256 for both profiles. 

**10. Should the cert-service manage tenant certs too?**
*Ruling (Locked to Codex):* Keep v1 narrow. The cert-service issues node, operator (all sub-roles), observer, break-glass, and short-lived query/session credentials (Toron-originated). It does *not* act as a general tenant application CA in v1. 

**11. Query-session cert revocation at IdP logout.**
*Ruling:* Rely on the 15-minute short lifetime and Toron heartbeat-based revocation. Explicit logout-triggered revocation is best-effort.

**12. Predicate-pushdown identity propagation.**
*Ruling:* Compute executes under the query-session cert's identity. 

**13. Break-glass issuance model (Dual-Auth vs Single-Auth).**
*Ruling (Concession to Claude):* Dual-auth is required for issuance of `tenant_ops`, `cluster_ops`, `key_ops`, and `full` scopes. I concede to Claude's exception: single-auth is permitted *only* for the `diagnostic` scope, provided it is rigidly constrained by the dispatcher to a 15-minute TTL, strictly read-only execution, forced `break_glass=true` audit flags with mandatory reason strings, and automatic paging of a secondary operator.

**14. Cert-service consensus substrate.**
*Ruling (Aligned with Claude's fallback):* Prototype `openraft` reuse in Phase 0 Week 4-6. If the prototype reveals the issuance log / CRL state machine is a poor fit or threatens delivery, formally fall back to the KMS-lock-based leader election design. We commit to the decision gate at Phase 0 exit.

**15. IdP integration breadth for Gate 4.**
*Ruling (Locked to Codex):* One major IdP (Okta or Azure AD) plus generic SAML path is the Gate 4 minimum. A second major IdP is shipped only if triggered by a specific customer deal.

---

## Workstream D — Audit, Evidence, and Retention (Claude)

**1. Sampling policy for range queries.**
*Ruling:* 100% query-start always. 100% completion for regulated tenants.

**2. Is `compliance_tier` a static tenant attribute or dynamic?**
*Ruling:* Dynamic upward, controlled downward. 

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
*Ruling:* Emit an explicit `cluster.rebuild`/fork marker.

**10. Pseudonymization for GDPR — when?**
*Ruling:* Gate 5 / regulated GA by default. 

**11. Operator-facing verification UX.**
*Ruling:* CLI summary mode is enough for pilot. 

**12. Trace-stitching for Toron-originated audits.**
*Ruling:* Correlate to the Toron-to-Wendelon request trace. 

---

## Workstream E — FIPS 140-3, Supply Chain, and Release Compliance (Codex / Claude)

**1. What exact pilot language is acceptable if CMVP is in progress but not complete?**
*Ruling:* "FIPS-ready build; CMVP validation in progress; not yet FIPS-certified." 

**2. Is `rustls-aws-lc-rs` mature enough for the target regulated-pilot window?**
*Ruling:* Plan for it as the primary path, but strictly maintain the fallback posture in the release plan until the transport-boundary path is empirically proven.

**3. What minimum SLSA target do we require before regulated pilot versus regulated GA?**
*Ruling:* SLSA L2 for Pilot. SLSA L3 for GA.

**4. Which customer-facing compliance artifacts are mandatory for the first regulated pilot, and which can wait for GA?**
*Ruling:* Keep the mandatory global bundle small: pilot evidence packet, SBOM, signed release artifacts + verification guidance, FIPS posture statement, and security architecture + NIST 800-53 control mapping summary. Sector overlays (SOC 2, PCI, HIPAA, FedRAMP) are explicitly customer- or sector-triggered.

**5. What platform set do we certify first, and which platforms are explicitly out of scope until a later validation cycle?**
*Ruling:* Linux x86_64 and aarch64. Windows, macOS, and BSD are explicitly out of scope for v1.

---

## Workstream A: Closed-Loop Healing, DR, and Operator Tooling (Gemini)

**1. Backup Storage Provider:**
*Ruling:* Backups must live separately from the live L2 tier (different bucket/account boundary).

**2. Operator Scope:**
*Ruling:* `wendelon-operator` manages the Wendelon storage tier ONLY. 

**3. Cross-Region Active Replication & 4-Hour RTO:**
*Ruling:* Warm standby or async cross-region replication is required for full-scale regulated SLAs. We will assign a single owner for the async-replication workstream before Phase 1 Week 2, with tenant-aware L2 replication and an explicit regulated-tenant RPO target as the minimum deliverable. 

**4. Cold-Restore Exception Contract Appendix:**
*Ruling:* If cold-restore is permitted for a smaller pilot, it requires an explicit contract appendix with: Maximum cluster size (≤10 TB), Maximum data volume per tenant (1 TB), Target RTO/RPO (best-effort, ~4h/1h), and explicit customer signoff language waiving RTO-breach liability.

**5. Gate 3 Operator and Restore Bar:**
*Ruling (Concession to Codex):* Not strictly mandatory for Gate 3. A rehearsed cluster-scoped restore and validated replacement automation contract (e.g., bash/Terraform) is acceptable for early Gate 3 pilots, provided it meets the same MTTR SLA. The native K8s operator remains a hard requirement for Gate 4 (regulated pilot).

---

## Workstream B: Toron Compute Tier & Ecosystem Integration (Gemini)

**1. Predicate Pushdown Security:**
*Ruling:* Do not allow arbitrary WASM-style compute pushdown in the first regulated pilot. Use a constrained, versioned compute AST.

**2. Quota Visibility:**
*Ruling:* Wendelon returns quota-consumption metrics in the HTTP/QUIC trailers of every successful response. Toron surfaces storage quota as a first-class query error condition. 

**3. Schema Evolution:**
*Ruling:* Toron owns schema intent; Wendelon owns durable schema state. `ALTER TABLE` must become a versioned proposal serialized through Wendelon's Raft state.

**4. Ecosystem Proof Bar:**
*Ruling:* Publish an explicit supported matrix for the pilot (e.g., top 3 client paths) and explicitly avoid generic compatibility claims. Commit to a maintained ingestion connector ONLY when a real pilot path necessitates it.

**5. Identity Mapping Ownership:**
*Ruling:* Toron owns the mapping translation; Wendelon owns the enforcement. The Wendelon Identity/Auth squad will author the integration test harness that Toron must pass.

---

## Workstream C: Transport, Admission Control & Runtime Stability (Gemini / Codex)

**1. Transport Truth-Up:**
*Ruling:* Retract the GSO/io_uring QUIC claims from the documentation for the pilot. 

**2. Buffer Pooling:**
*Ruling:* Prepare the sharded implementation in parallel, but measure contention post-`io_uring` split before shipping it. 

**3. Connection Fairness:**
*Ruling:* Enforce coarse IP/global token buckets pre-auth, then shift to tenant fairness post-auth.

**4. Is per-device worker mapping mandatory for Gate 2?**
*Ruling:* No. WAL/slab separation is mandatory for Gate 2. 

**5. Should QUIC admission control reject early or shed later?**
*Ruling:* Both. Reject early on global pre-auth saturation, then use bounded authenticated queues with shedding for post-auth fairness.

**6. Which live-path aborts are truly invariant failures?**
*Ruling:* Only memory-safety-adjacent impossible states and post-validation internal invariants. 

---

## Workstream D: Storage and Stellarator Correctness (Gemini / Codex)

**1. Leader Fencing Thresholds:**
*Ruling:* If the local persistence path fails with integrity-threatening errors, step down immediately.

**2. Client Retry Semantics:**
*Ruling:* Normalize into a stable retryable client error with `Retry-After` metadata.

**3. Orphaned Remote Data:**
*Ruling:* Mark it for asynchronous cleanup through the sweeper path. Do not synchronously block the failed write.

**4. Is local durable append a hard requirement before any quorum success can be reported?**
*Ruling:* Yes, if the local leader is a member of the replica set. Do not allow pure-remote quorum in that case.

**5. Separate background finalization worker or existing wakeup/replay path?**
*Ruling:* Use a dedicated background finalization/replay worker. 

---

## Workstream E: Observability, Validation, and SLO Evidence (Gemini / Codex)

**1. Cardinality Limits:**
*Ruling:* No unbounded per-tenant labels in default exported metrics. 

**2. Linearizability Harness:**
*Ruling:* Build the narrower in-house checker first, then run Jepsen before the regulated pilot bar (Gate 4).

**3. Trace Sampling:**
*Ruling:* Tail-based sampling in the collector. Keep 100% errors, writes, and finalization failures. Keep traces over the target latency threshold, and sample a low baseline (1-5%) of successes.

**4. What exact soak duration and load shape are required before promoting an SLO?**
*Ruling:* Minimum of four consecutive weekly soaks, each with at least 24 hours at representative mixed load, plus one 72-hour release-candidate soak.