# Wendelon Enterprise Hardening & Pilot Execution Plan
**Date:** April 22, 2026 (v2 — Updated with cross-functional execution reviews)
**Author:** Lead Software Architect (Gemini)
**Objective:** Address the critical scaling, operational, and security gaps identified in the readiness assessment (`plans/gap/gemini_eval_20260422.md`) to transition Wendelon from an architectural prototype to a Fortune 500 mission-critical enterprise deployment.

---

## 1. Execution Philosophy & Program Rules

To close the gap between our current readiness score and full enterprise pilot authorization, we must shift our engineering focus from *architectural purity* to *operational reality*. I have incorporated execution strategies from the cross-functional leadership team (Claude & Codex) to establish the following non-negotiable program rules:

1. **No Design Claim Survives a Code Mismatch:** If a comment, ADR, or operator doc claims behavior X and the code does Y (e.g., "one `io_uring` thread per NVMe", "concurrent remote fan-out"), one of them must change *in the same PR*.
2. **Correctness Before Throughput:** Crash recovery and deterministic state transitions must land before latency tuning.
3. **Resolve Ambiguity Before Implementation:** No dependent implementation code lands while its load-bearing semantics are "Proposed." ADRs must be moved to "Accepted" by closing open questions first.
4. **Validation is Binary and Automated:** A gate is either complete or not. No workstream claims completion without passing against the automated CI Program Scorecard.

---

## 2. Phased Execution Roadmap

The remediation work is organized into sequential phases, governed by strict exit criteria.

### Phase 0: Baseline, Governance, & Calendar (Weeks 1-4)
*Target: Establish the CI Program Scorecard, capture baselines, and unblock calendar-dominant dependencies.*

*   **Task 0.1 (Calendar Dependencies):** Kicked off on Day 0. Begin CMVP lab contracting for FIPS certification, file IANA PEN for enterprise OIDs, and sign legal vendor contracts for AWS/GCP/Azure KMS integrations.
*   **Task 0.2 (Architecture Lock):** Close all Open Questions on the critical ADRs (018, 020, 025, 026) and formally promote them from Proposed to Accepted.
*   **Task 0.3 (CI Program Scorecard):** Build the automated scorecard. Capture latency baselines for Raft commits, read-admission latency, and Stellarator writes.
*   **Task 0.4 (Fault Harness):** Build the 1,000-iteration crash harness targeting Stellarator partial-failure windows (pending-intent failure, remote stall, etc.).

### Phase 1: Scale & Survive (Weeks 5-10)
*Target: Ensure the code matches architectural scaling and concurrency claims.*

*   **Task 1.1 (`io_uring` Topology Alignment):** Split the shared `io_uring` thread. Provision one dedicated ring for the Raft WAL and one ring per physical NVMe device for slab I/O.
*   **Task 1.2 (Stellarator Concurrency & Recovery):** Refactor the bifurcated write path for true concurrent remote replica fan-out. Implement Codex's explicit write-outcome state enum (`DataBranchPending`, `DurableQuorumReached`, `FinalizationCommitted`, `FinalizationRecoverablePending`, `FinalizationTerminalFailure`). Make pending-intent persistence mandatory; remove the fallback "best-effort" recovery branch.
*   **Task 1.3 (Request Admission & Panic Inventory):** Replace the full `SmState` clone in request validation with a narrow, immutable `tokio::sync::watch` cache. Conduct a live-path inventory of `expect()` and poisoned-lock assumptions, converting recoverable sites to structured errors.
*   **Task 1.4 (QUIC Tuning):** Implement strict QUIC admission controls: max concurrent connections, bounded task fan-out, and per-tenant backpressure queues. Retract or implement the "GSO/io_uring transport" claim.

### Phase 2: Procurement Ready (Weeks 11-16)
*Target: Pass Fortune 500 procurement, SOX/PCI, and FIPS pre-audits.*

*   **Task 2.1 (Enterprise Key Custody):** Deprecate the `dev-insecure` Root KEK provider. Integrate AWS KMS, GCP KMS, and Azure Key Vault behind the ADR 020 abstraction. Document the M-of-N custodian escrow ceremony.
*   **Task 2.2 (RBAC Dual-Authorization):** Split the monolithic `operator` role into granular personae (`tenant_admin`, `cluster_admin`, `compliance_auditor`). Implement M-of-N signature requirements for destructive Raft proposals (e.g., tenant deletion, break-glass issuance).
*   **Task 2.3 (Audit Completeness):** Expand ADR 025 to include `data.read` and `data.write` events. Promote `audit-kafka` to default build inclusion, canonicalizing the payload to include full tamper-evident chain hashes.
*   **Task 2.4 (FIPS Maturation):** Ensure transport TLS utilizes a FIPS-validated module (`rustls-aws-lc-rs`). Provide an accurate symbol sweep and KAT evidence suite.

### Phase 3: Pilot Authorization (Weeks 17-20)
*Target: Provide operators with tools to manage 99.999% uptime and integrate with the broader enterprise stack.*

*   **Task 3.1 (Closed-Loop Cluster Healing):** Ship a first-party Kubernetes Operator that consumes Wendelon's eviction metrics and synchronously provisions replacement nodes to eliminate degraded durability windows.
*   **Task 3.2 (Toron & Ecosystem Integration):** Publish `ops/toron_integration.md`. Validate end-to-end integration covering SQL gateways, JDBC/ODBC compatibility, and SSO/SAML mapping to Wendelon mTLS OIDs. Document ELT ingest patterns (Fivetran, dbt).
*   **Task 3.3 (Disaster Recovery Choreography):** Author a comprehensive DR plan outlining RTO and RPO SLAs. Build tooling to coordinate cluster-wide consistent snapshots rather than single-node images. Validate via a full cross-region restore drill.
*   **Task 3.4 (Lazy Migration QoS):** Introduce adaptive QoS rate-limiting on "Migration Reads" per source-range and proxy-node to prevent thundering herds after topology changes.

---

## 3. Milestone Gates & Validation Criteria

Movement to the next phase is strictly gated by the following validations:

*   **Gate 0 (End of Phase 0): Baseline Complete**
    *   *Validation:* Scorecard live. IANA PEN filed. ADRs Accepted. No engineering begins on Track 1 until Gate 0 is green.
*   **Gate 1 (End of Phase 1): Hot-Paths Hardened**
    *   *Validation:* The 1,000-iteration injected crash/failure harness completes without a single stranded quorum-durable write. Raft commit-latency p99 is proven decoupled from concurrent slab-write load.
*   **Gate 2 (End of Phase 2): Security Cleared**
    *   *Validation:* Third-party or internal red-team audit of the OID authorization logic, dual-authorization enforcement, and key custody path. `dev-insecure` is mechanically impossible to load in default profiles.
*   **Gate 3 (End of Phase 3): Ready for Customer Traffic**
    *   *Validation:* **Full teardown-and-restore of a test cluster using ONLY the documented runbooks.** This must be performed by an engineering team that has never operated the cluster. Any required "tribal knowledge" results in a gate failure.