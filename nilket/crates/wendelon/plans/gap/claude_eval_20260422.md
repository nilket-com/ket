# Wendelon Fortune-500 Enterprise Gap Analysis

**Auditor:** Principal Cloud Architect / Security Auditor (Claude)
**Date:** 2026-04-22 (v2 — integrates council review from `codex_20260422.md` and `gemini_20260422.md`)
**Scope:** Architecture and security posture of Wendelon (philosophy, ADRs 018/019/020/021/025/026/027/029, ops/audit, ops/backup_dr, ops/key_rotation, ops/multi_tenancy, ops/observability, ops/upgrades, ops/lifecycle, ops/durability, ops/ring_management, network/security) against Fortune-500 regulated-industry adoption criteria (financial services, healthcare, critical infrastructure). The v2 pass additionally incorporates Codex's `src/`/`tests/` code-level audit and Gemini's operational/ecosystem assessment.
**Verdict (overall):** **CONDITIONAL — NOT YET PRODUCTION-READY for regulated Fortune-500.** The cryptographic posture, tenant-aware schema, and audit-event model are genuinely above-average for a pilot, but there are five design-level blocking gaps (marked **CRITICAL** below) plus two code-level critical gaps surfaced by Codex (storage topology does not match the per-device `io_uring` claim; finalization recovery degrades to best-effort on pending-intent persistence failure) that together would fail a procurement security review in financial services or healthcare. The "Unified Ring" philosophy is defensible as an engineering choice but creates *positioning* friction with enterprise isolation expectations that must be addressed in documentation and role-design even though it is not re-litigated at the architecture level.

---

## 1. Methodology

This review performs a design-level gap analysis. It does not exercise code. v2 additionally folds in Codex's code-level evidence (line-anchored citations into `src/` and `tests/`) and Gemini's operations/ecosystem lens (Day-2 ops, compute-tier integration, ELT ecosystem). Where the peer reviews ran contrary to v1 or added evidence v1 did not see, the risk matrix and domain sections below have been updated; v1 findings that stand are preserved.

Assessment is against:

- **Control frameworks:** NIST SP 800-53 Rev. 5 (AC, AU, CM, CP, IA, SC, SI families), PCI-DSS 4.0, HIPAA Security Rule (45 CFR §164.3xx), SOX §404 ITGC, FedRAMP Moderate/High baselines, FFIEC IT Handbook.
- **Cryptographic baselines:** FIPS 140-3, NIST SP 800-38D (AES-GCM), SP 800-57 (key management), SP 800-131A (algorithm transitions).
- **Enterprise expectations:** separation of duties, least-privilege RBAC, HA/DR with measured RTO/RPO, key lifecycle with escrow and rotation governance, tamper-evident audit extending beyond admin actions, data-access auditability, data residency, right-to-erasure.

Evidence cites the plans/ tree in this repository as of commit `c85bc49` (HEAD of `main`, 2026-04-22).

---

## 2. Executive Summary

### What is genuinely strong
- **Identity/Locality split (ADR 029).** 256-bit BLAKE3 for storage identity, 64-bit projection for routing, with newtype enforcement. Collapses to one ring in mental model while fixing the 64-bit collision risk at warehouse scale. This is better than most distributed-database peers ship.
- **Dual-CA mTLS with OID-bound authorization (ADR 018).** Cryptographically binds tenant + role to the handshake. Eliminates bearer-token lifecycle drift. Break-glass is explicitly tagged.
- **Per-slab DEKs + three-tier hierarchy (Root KEK → Tenant KEK → Slab DEK) with chunked AEAD and bit-exact AAD (ADR 020).** The injective nonce derivation is correct; the load-bearing invariants are documented; the FIPS branch degrades cleanly.
- **Audit model (ADR 025).** Canonical structured schema from day one, per-node hash chain with signed 60s roll-up, two authorities (RaftLog vs NodeLocal) with honest atomicity semantics, fail-closed on NodeLocal. This is materially better than "ship NDJSON and hope."
- **Day-one tenant-aware schema (ADR 021).** Retrofit risk is eliminated; every durable structure carries `target_tenant_id`; dispatcher pairs actor cert with request target.
- **Observability is complete at pilot (ADR 027).** OTLP, histogram metrics, three-listener posture, mTLS on both `/ready` and `/metrics`, cardinality discipline. Not "secure later."

### What blocks Fortune-500 adoption today

**Design-level CRITICAL gaps (from v1, detailed in §4):**

1. **Root KEK integration is still the `dev-insecure` placeholder** (persisted plaintext at `<data_dir>/.wendelon/dev-root-kek.bin`). Production KMS/HSM integration is not shipped. `ops/key_rotation.md` explicitly states this. (Codex corroborates at `src/crypto.rs:104-107` — the enum literally has one variant, `DevInsecure`.)
2. **No key-escrow / key-recovery path.** `ops/key_rotation.md`: *"Loss of every valid Root KEK unwrap path is unrecoverable for encrypted data in this pilot."* For regulated data under litigation-hold or e-discovery, this is unacceptable without HSM-backed escrow or M-of-N recovery.
3. **Transport TLS is not FIPS-validated.** ADR 026 Open Question — `rustls`/`ring` has no FIPS story; the current ADR explicitly leaves transport outside the module boundary. FedRAMP High / DoD SRG IL5+ fail on this alone.
4. **No data-access audit.** ADR 025's closed operation enum covers admin/auth events only — data reads and writes emit no audit records. HIPAA §164.312(b) and PCI-DSS §10.2.1 require data-access logging; this is a structural miss, not a parameter gap.
5. **Backup/DR is single-node only with no PITR, no cross-region choreography, no RTO/RPO.** `ops/backup_dr.md` itself flags most of this as deferred. A bank with a contractual four-hour RTO cannot deploy against this document. Gemini further sharpens this into a "Level-2 healing gap": the cluster preserves availability via eviction but runs at degraded replication factor until external provisioning (K8s, cloud SDK) replaces the node, and no sidecar/operator is shipped to make that replacement synchronous.

**Code-level CRITICAL gaps (new, from Codex's `src/` audit):**

6. **Storage topology does not match the per-NVMe `io_uring` claim.** `src/storage/io_uring.rs:3-5` documents "one such pair per physical NVMe device"; `src/node.rs:970-996` actually spawns one shared ring thread per node and multiplexes Raft WAL durability and slab I/O onto it. Commit latency and slab-write latency are directly coupled on that one queue. Several file operations (`src/storage/file_ops.rs:141-183, 241-305`) still go through blocking worker threads. This is the dominant hot-path performance misalignment in the current build.
7. **Finalization recovery becomes best-effort when pending-intent persistence fails.** `src/network/data_server.rs:3402-3447` explicitly continues after `pending_finalizations.upsert(...)` returns an error, and the restart-replay bookkeeping is only installed on the success branch. A storage or local-metadata hiccup in the wrong window can strand quorum-durable data without deterministic finalization replay — precisely the partial-failure class a bifurcated write model must nail.

**Operational / ecosystem CRITICAL gaps (new, from Gemini):**

8. **No synchronous node-replacement path.** "Level-1" mathematical eviction ships; "Level-2" infrastructure provisioning is explicitly rejected (ADR 001). Until replacement happens, the cluster runs at degraded durability. For a 99.999% SLO this is a window-of-vulnerability that is unacceptable in the steady state. Gemini's remediation — a first-party K8s Operator or tightly-coupled sidecar — is consistent with Wendelon's "don't embed cloud SDKs" posture and should be committed pre-pilot.
9. **No compute-tier integration runbook.** Wendelon ships a storage tier; enterprise procurement expects SQL / JDBC / ODBC / SSO / SAML. These belong to the Toron compute tier, but the end-to-end operational boundary between Wendelon and Toron is not documented or integration-tested. A "storage-only" enterprise pilot is not a credible enterprise pilot.

### Posture by domain

| Domain | Rating | Rationale (one line) |
|---|---|---|
| Cryptographic primitive selection | **A-** | GCM-SIV default, plain-GCM FIPS branch with injective nonces, aws-lc-rs — solid. |
| Key lifecycle (production) | **D** | `dev-insecure` Root KEK provider; no escrow; rotation has no operator surface. |
| Identity / mTLS / RBAC | **B** | Dual-CA is good; four roles too coarse for regulated sep-of-duties; OID PEN still TBD. |
| Multi-tenancy isolation | **B-** | Logical isolation strong; physical isolation is post-pilot staged work. |
| Audit (admin events) | **B+** | Tamper-evident chain solid; sink-shape asymmetry and retention defaults are gaps. |
| Audit (data access) | **F** | Not in the event model. |
| HA / Durability | **C** | Raft + L1/L2 tiering sound; durability SLO is "TARGET (unvalidated)". |
| Disaster recovery | **D** | Single-node snapshot only; no PITR, no multi-node drill, no RTO/RPO. |
| FIPS 140-3 | **C** | Build profile and symbol sweep exist; CMVP number absent; transport outside boundary. |
| Observability (pipeline) | **A-** | Three-listener, OTLP, cardinality discipline — best-in-class at pilot stage. |
| Observability (coverage) | **C** | Codex: missing pending-finalization, replica-fanout, `io_uring` depth, WAL fsync, connection/task, lock-contention metrics. |
| Incident response / governance | **D** | Break-glass exists; IR plan, runbooks, dual-control workflows absent. |
| Supply-chain / build integrity | **F** | No SBOM, no reproducible build evidence, no artifact signing documented. |
| Code-level hot paths | **D+** | Codex: shared `io_uring`, Stellarator local-before-remote serialization, full `SmState` clone per request, no QUIC admission control. |
| Compute-tier / ecosystem integration | **D** | Gemini: no documented Toron end-to-end, no ELT connector story, Kafka audit sink behind cargo feature flag. |
| Day-2 provisioning / healing | **D** | Gemini: Level-1 eviction only; no synchronous node-replacement sidecar/operator; degraded-durability window after eviction. |
| Test / fault-injection depth | **C-** | Codex: chaos smoke is 3 scenarios, simulator ladder is one named scenario, FIPS "sweep" is symbol/string regex. |

### Pilot Readiness Score

**v2 headline: ~50% ready for a regulated Fortune-500 pilot** (revised down from v1's 55% after Codex's code-level evidence on shared-ring `io_uring`, Stellarator serialization, hot-path `SmState` clone, and finalization best-effort recovery; Gemini's Level-2 provisioning gap further reduces the Day-2 column).

For the project's own self-defined single-tenant engineering pilot — where `dev-insecure` KMS, unvalidated SLOs, and Proposed-not-Accepted ADRs are acceptable — readiness is in the **65–75%** range. The gap between those two numbers is operational, governance, and code-hardening work, not architecture.

**Council score consensus:**

| Reviewer | Lens | Regulated / customer-facing pilot | Internal / design-partner pilot |
|---|---|---|---|
| Claude (v1) | ADRs + ops docs (design-level) | 55% | 75% |
| Codex | `src/` + `tests/` (code-level) | 48% | 62% |
| Gemini | Ops + ecosystem (enterprise fit) | ~45% (implied) | 65% |
| **Consensus band** | — | **~48–55%** | **~62–75%** |
| **Claude v2 (integrated)** | all three lenses | **~50%** | **~68%** |

Three independent review lenses converge on a mid-40s-to-mid-50s band for regulated pilot. That agreement is notable: Claude's lens is procurement/controls, Codex's is hot-path correctness, Gemini's is Day-2 ops. Each found different evidence; the headline number moved less than the underlying evidence would suggest because each reviewer was pricing in the gaps the other two quantified, even when they couldn't cite them precisely.

Domain breakdown against the regulated-F500 pilot bar (v2 — integrates Codex & Gemini):

| Domain | % | Primary limiter |
|---|---|---|
| Observability (pipeline) | 90 | Runbooks empty |
| Crypto primitives | 85 | aws-lc-rs CMVP number not recorded |
| Audit (admin events) | 75 | Sink-shape asymmetry; 90-day retention default; break-glass schema |
| Multi-tenancy (logical) | 70 | Physical isolation is post-pilot staged work |
| Core arch / Raft integration | 70 | Codex: state-machine placeholders (`SchemaUpdate` unmodeled), control-ring wire contracts not frozen |
| mTLS / Identity | 60 | OID PEN unregistered; cert-rotation service and CRL distribution undesigned |
| Storage durability path | 58 | Codex: shared `io_uring`, blocking file ops, scaffold-grade copies in hot paths |
| RBAC | 55 | `operator` role too broad; no dual-authorization |
| FIPS | 55 | Transport outside the module boundary; no CMVP certificate; symbol-sweep ≠ compliance |
| Stellarator bifurcated writes | 50 | Codex: local-before-remote serialization; best-effort finalization recovery |
| QUIC transport / hot-path efficiency | 50 | Codex: no admission control, no per-tenant bounds, tier-name-claims-not-in-code, copy-heavy framing |
| Test / fault-injection depth | 45 | Chaos smoke (3 scenarios), simulator ladder (1 scenario), FIPS sweep is string check |
| Observability (coverage) | 45 | Missing pending-finalization, quorum fan-out, io_uring depth, WAL fsync, connection count |
| Ecosystem / compute-tier integration | 40 | No Toron runbook, no ELT connectors, Kafka sink cargo-feature-flagged |
| HA / DR | 35 | No PITR; no cross-region; no RTO/RPO; no synchronous node replacement |
| Key lifecycle (production) | 25 | `dev-insecure` Root KEK; no escrow; no operator surface |
| Governance / IR | 20 | No IR plan; ADRs all Proposed |
| Supply chain | 15 | No SBOM; no signed artifacts |
| Audit (data access) | 10 | Structurally absent from the event model |

The architecture-to-operations ratio is still the right shape: the hard-to-retrofit components (Identity/Locality, envelope hierarchy, tenant schema, hash-chained audit) are genuinely strong, and Codex's independent code-level read confirms the major subsystems are implemented, not merely sketched. But the v2 evidence shows that several *hot paths* within those subsystems are still at scaffold grade — the architecture is correct, the implementation tier is not uniformly production-ready.

---

## 3. "Unified Ring" vs. Traditional Enterprise Isolation — A Positioning Assessment

Wendelon's `plans/philosophy.md` rejects the control-plane / data-plane split as *architecturally forbidden* (enforced by `tests/architecture_test.rs`). The engineering argument is coherent: Identity/Locality is a *precision split of one hash space*, not a topology split. The ring stays one ring; separation lives in roles and types rather than in separate listeners or separate clusters.

However, the enterprise mental model that Wendelon disavows is **not primarily an engineering preference** — it is driven by three non-engineering concerns that will appear in every Fortune-500 procurement review:

### 3.1 Blast-radius concerns (security)
In a split architecture, compromise of a data-plane host does not yield membership-change authority; that authority lives on a separately-firewalled control plane. In Wendelon, every node is a peer on the same ring, distinguished only by the role embedded in its mTLS cert OID. Role separation is cryptographic and enforceable (ADR 018 §6 enforcement-at-dispatcher), which is defensible, but **this is a role-level split inside one network fabric, not a network-level split**. A procurement review looking for evidence of "management network isolated from data network" will not find it.

**Remediation required:**
- Author a white-paper-grade mapping that explicitly demonstrates how the OID-role-split satisfies NIST SP 800-53 **AC-4 (Information Flow Enforcement)**, **SC-7 (Boundary Protection)**, and **PCI-DSS §1.3 (network segmentation)** control objectives. Do not assume reviewers will accept the philosophical argument; translate it to the control framework.
- Document a *recommended network topology* that places operator-originated admin RPCs behind a bastion, even though the protocol does not require it. Give procurement a network diagram that looks conventional.
- Consider an optional deployment mode where `operator` and `break_glass_admin` RPCs are accepted **only** on a listener bound to a cluster-internal admin VLAN — the handshake still decides posture, but the listener binding gives firewall policy something to enforce. This is a documentation and config change, not an architecture break.

### 3.2 Separation-of-duties concerns (governance)
Fortune-500 controls (COBIT 2019 DSS05, FFIEC IT Handbook, SOX §404 ITGC) require **at minimum** four non-overlapping personae: Database Administrator, Security Officer, Compliance Auditor, Platform Operator. Wendelon's four pilot roles (`data_node`, `operator`, `read_only_observer`, `break_glass_admin`) collapse DBA+Operator into one `operator` role with extremely broad authority: *"all `data_node` permissions plus membership changes, epoch advances, lease grants, leadership transfer, drain, backup/restore, tenant admin"* (ADR 018 §3).

A single operator cert in a 9am phishing window is therefore a full-cluster takeover including cross-tenant data access. This is not hypothetical — it is exactly what the `operator` role authorizes by design, because the cert carries `tenant_id = null` and the dispatcher admits any target.

**Remediation required:**
- **Split `operator` into at least three roles** for regulated deployments: `tenant_admin` (tenant lifecycle, quota), `cluster_admin` (membership, epoch, leadership), `backup_admin` (backup/restore, key rotation). None implies another; combined authority requires two cert holders working together (M-of-N on destructive ops).
- **Add a `compliance_auditor` role** distinct from `read_only_observer`. Auditor can read audit tail and `/ready` but not metrics that expose payload-size timeseries. Observer can read metrics but not audit tail. Today, `read_only_observer` reads both (ADR 018 §3 + ADR 025 §3 chain-head RPC).
- **Add dual-authorization (2-person control) gating on destructive operations:** `delete_tenant`, KEK destruction, membership eviction of last healthy node, break-glass-cert issuance. Today ADR 021 §5 only requires one operator signature. Dual-control is achievable within the OID model by requiring two valid certs on the same Raft proposal — a pilot-era change, not a re-architecture.
- **Just-in-time (JIT) operator authority.** Today operator certs last 8 hours by default. Offer a shorter default (30–60 minutes) with an explicit elevation RPC that emits an audit record at elevation. Procurement maps this to NIST SP 800-53 AC-2(11) account use conditions.

### 3.3 Regulatory-separation concerns (data residency, physical isolation)
ADR 021 §1 is explicit: *"Isolation scope does not extend to physical resource isolation at day one."* Tenants share CPU, disk bandwidth, network, cache memory. Per-tenant replication factor, per-tenant L2 bucket, and per-tenant Root KEK are staged Phases 1–3 with **no schedule**.

Fortune-500 deployments in healthcare (PHI tenants adjacent to non-PHI), finance (CDE tenants adjacent to non-CDE, PCI-DSS §1.2), and multi-jurisdictional SaaS (EU vs. APAC residency under GDPR / Schrems II) routinely require **physical or at-minimum infrastructure-level isolation** per tenant tier. Logical isolation at the ring layer is necessary but not sufficient for these buyers.

**Remediation required:**
- **Commit Phase-1 per-tenant replication factor to the pre-GA scope,** not post-pilot. This is the smallest change (ADR 021 §8 confirms it is placement-only) and unlocks "critical tenants get 5x replication" sizing that procurement asks for.
- **Phase-2 per-tenant L2 bucket is a data-residency blocker.** Without it, one region's L2 bucket stores every tenant's ciphertext; residency policy cannot be enforced at the storage layer. Move this to pre-GA for regulated-industry SKUs.
- Until Phase-3 per-tenant Root KEK ships, document a compensating control: *"Tenants requiring cryptographic isolation from other tenants on the same cluster run on a dedicated single-tenant cluster."* This is honest and operational, not architecture-shifting.

### 3.4 What does *not* need to change
The `plans/philosophy.md` unification principle is load-bearing for the engineering simplification Wendelon delivers. The remediations above are additive documentation and role-configuration work; none of them require re-introducing a control-plane/data-plane topology split. The answer to "where is your control plane?" is not "we don't have one" — it is *"role-level separation at the OID layer with network-level separation at deployment, documented to map to control objectives X/Y/Z."*

### 3.5 Scaling-side concerns raised by Gemini (operational, not architectural)

Gemini's review surfaced two scaling concerns against the unified-ring model that are not architecturally blocking but should be priced into the pre-GA plan:

- **Capability-filter CPU overhead at thousands-of-vnodes scale.** Routing on the unified ring requires dynamic filtering for node capabilities (`data_node` vs `operator`-accessible, tenant-scoped vs cross-tenant). At fleet sizes where vnode counts are in the tens of thousands, this filtering compounds on every placement decision. Benchmark evidence before regulated-SKU GA. If filtering is measurable, cache the per-epoch capability sets instead of recomputing per decision.
- **Hash-algorithm immutability has petabyte-hostile failure mode.** `ops/ring_management.md` §3 states: any change to the locality-hash algorithm or parameters requires a new cluster (`Cluster B`), dual-write, full table scan, cutover, destroy `Cluster A`. At petabyte scale this is operationally prohibitive. The invariant is correct (in-place hash change is catastrophic) but the cost of the escape hatch needs honest pricing in the customer-facing SLA. A related mitigation: over-provision the 64-bit Locality projection headroom *now* so parameter changes are far less likely to be required within the cluster's supported lifetime.

---

## 4. Prioritized Risk Matrix

Severity × Likelihood × Regulatory-impact. CRITICAL = procurement stopper in regulated Fortune-500; HIGH = will appear in first-round audit findings; MEDIUM = compensating control acceptable for pilot; LOW = track for GA.

| # | Finding | Severity | Domain | Evidence | Remediation summary |
|---|---|---|---|---|---|
| R-1 | `dev-insecure` Root KEK is the only provider; plaintext at `<data_dir>/.wendelon/dev-root-kek.bin` | **CRITICAL** | Key lifecycle | `ops/key_rotation.md` "Scope and Current Limits" | Ship AWS KMS + GCP KMS + Azure Key Vault + PKCS#11 (HSM) providers before any production deployment. Close ADR 020 Open Question on KMS backend abstraction. |
| R-2 | No key escrow / recovery workflow; KMS unavailability = permanent data loss | **CRITICAL** | Key lifecycle / DR | `ops/key_rotation.md` "No-Escrow Posture" | Design M-of-N custodian escrow for Root KEK (Shamir split across HSM quorum). Document quarterly key-ceremony. This is distinct from backup/restore. |
| R-3 | Transport TLS (`rustls`+`ring`) is outside the FIPS module boundary | **CRITICAL** | FIPS / Cryptography | ADR 026 §2, Open Question "Transport TLS FIPS story" | Land `rustls-aws-lc-rs` transport provider; re-run full ADR 019/026 acceptance. Do not ship "FIPS 140-3 certified" SKU until transport is inside the boundary. |
| R-4 | No data-access audit (reads/writes) — closed event enum covers admin/auth only | **CRITICAL** | Auditability | ADR 025 §1 operation enum | Extend audit model with `data.read` / `data.write` event types, emitted from the dispatcher with tenant_id + record Identity + actor. Add sampling policy; at 100% rate, design retention and sink costs. |
| R-5 | Backup / DR is single-node snapshot; no PITR, no cross-region choreography, no RTO/RPO | **CRITICAL** | HA / DR | `ops/backup_dr.md` entire document; "Deferred" section | Author DR plan with quantitative RTO/RPO. Implement continuous L2 cross-region replication (or document the customer-owned equivalent). Ship cluster-wide restore choreography and tested drill evidence. |
| R-6 | `operator` role is excessively broad (DBA+Platform+Backup+TenantAdmin collapsed) | HIGH | RBAC / Separation of duties | ADR 018 §3 role definitions | Split into `tenant_admin`, `cluster_admin`, `backup_admin`, `compliance_auditor`. Enforce deny-by-default at dispatcher (already architected). |
| R-7 | No dual-authorization (2-person control) on destructive operations | HIGH | RBAC / Governance | ADR 018, ADR 021 §5 | Require two distinct operator cert signatures on `delete_tenant`, KEK destruction, quorum-affecting membership changes, and break-glass issuance. Enforce in Raft proposal validation. |
| R-8 | OID prefix is "TBD at ADR acceptance"; IANA PEN not registered | HIGH | Identity | ADR 018 Open Questions | Register PEN and freeze OID *before* any pilot deployment outside the vendor. Every cert issued before freeze will require reissuance. |
| R-9 | Audit retention defaults to 90 days; regulated industries need 7 years | HIGH | Auditability / Compliance | ADR 025 §6 | Change default to 2555 days (7 years). Add a retention-policy config surface with min-floor enforced at tenant level (cannot go below the tenant's compliance tier). Ship WORM/object-lock integration for off-box archive. |
| R-10 | Audit sink shapes are asymmetric; Kafka/Syslog lack chain fields | HIGH | Auditability / SIEM integration | `ops/audit.md` §3–§4 | Ship a canonical audit projection with chain fields (sequence_number, prev_hash, record_hash, hash_algorithm_tag) available on Kafka. Syslog stays minimal. Document the compliance-evidence archive as local-NDJSON-shipped-to-WORM, not Kafka. |
| R-11 | Tenant KEK rotation has no operator surface (no CLI, no admin RPC) | HIGH | Key lifecycle | `ops/key_rotation.md` "Rotation Entry Point" | Land a `TenantAdmin.rotate_tenant_kek` RPC with operator-role gating, audit emission, and dual-authorization. ADR 018/021 patterns already exist. |
| R-12 | Rotation does not re-wrap existing Slab DEKs; cold slabs keep old Tenant KEK wrap | HIGH | Key lifecycle | ADR 020 Open Question "Tenant KEK rotation cadence"; `ops/key_rotation.md` | Ship a re-wrap sweeper that runs alongside the plaintext→ciphertext sweeper. Cap time-to-full-rotation at a bounded SLA (e.g., 30 days). Historical KEK retirement gated on this SLA. |
| R-13 | Certificate rotation service (for 24h node certs, 8h operator certs) — placement is an Open Question | HIGH | Identity / Availability | ADR 018 Open Questions | Design and ship the cert-rotation service with an explicit no-deadlock property (e.g., uses a standalone HA service or accepts a graceful staleness window). Document HA topology. |
| R-14 | CRL distribution endpoint for Operator CA is an Open Question; 30-minute fail-closed ceiling declared without a distribution mechanism | HIGH | Identity / Revocation | ADR 018 Open Questions | Close via cluster-state-machine-distributed CRL (load-bearing per the fail-closed rule) or operator-run HA HTTP endpoint. Document the SLA on the endpoint. |
| R-15 | Decrypted slab plaintext is not zeroized on drop | HIGH | Key hygiene | `ops/key_rotation.md` "Scope Boundaries" | Wrap slab-plaintext buffers in `ZeroizeOnDrop` or a region allocator with zeroize. Coordinate with ADR 009 (AlignedBuf) for in-place zeroization before munmap. Required for a credible CMVP Security Policy. |
| R-16 | Break-glass certificates grant unrestricted authority for up to 1h with no pre-approval workflow | HIGH | Governance | ADR 018 §3 | Require dual-control on break-glass issuance (Operator-CA ceremony + second operator sign-off). Consider time-boxed scope, not just lifetime — a break-glass cert that can only perform a named operation class. |
| R-17 | ADR 025 event parameter schema for `break_glass.used` is still an Open Question | HIGH | Auditability | ADR 025 Open Questions | Close schema: invoking-cert-serial, human-name binding (external mapping), reason string (non-empty, length-bounded), incident-ticket-id. Block ADR 025 Accepted status on this. |
| R-18 | No WORM / object-lock integration for long-term audit archive | HIGH | Auditability | Not addressed | Add an `AuditSink` implementation that writes to object-lock-enabled S3 or equivalent. Verify the rotation-on-boundary path preserves chain continuity across the sink. |
| R-19 | No right-to-erasure (GDPR Art. 17) workflow; audit records post-tenant-delete still reference tenant_id | HIGH | Privacy / Compliance | ADR 021 §6, ADR 025 | Document the crypto-shredding position (destroying Tenant KEK renders payload unrecoverable). Design a separate audit-scrubbing or audit-pseudonymization workflow for GDPR-scope deployments, with legal review. |
| R-20 | Tenant quota has bounded overshoot by design | MEDIUM | Multi-tenancy / SLA | ADR 021 §4, `ops/multi_tenancy.md` §3.4 | Document the overshoot formula in the customer-facing SLA. For tenants where strict is required, ship the post-pilot reservation mechanism (pre-debit). |
| R-21 | Tenants share physical CPU/disk/network/cache; Phases 1–3 are post-pilot | MEDIUM | Multi-tenancy / Physical isolation | ADR 021 §1 | Commit Phase-1 per-tenant RF to pre-GA. Phase-2 per-tenant L2 bucket is a residency blocker; commit by regulated-SKU GA. |
| R-22 | No supply-chain security evidence: no SBOM, no signed binaries, no SLSA attestations | MEDIUM | Build integrity | Not addressed | Emit SPDX SBOM per build, cosign/sigstore artifact signing, SLSA Level 3 target. Required for DoD / FedRAMP High. |
| R-23 | Durability "11 nines" claims explicitly disclaimed; SLOs marked "TARGET (unvalidated)" | MEDIUM | Reliability | `ops/observability.md` §4 | Run the chaos+simulator harness (ADR 028) to downgrade TARGET → COMMITTED before customer SLA commitments. |
| R-24 | Mixed-build-profile (FIPS + default) clusters are explicitly unsupported | MEDIUM | FIPS / DR topology | ADR 026, `ops/upgrades.md` §4 | Accept as a documented operational constraint. Customers with mixed-sovereignty deployments must run parallel clusters per compliance domain. Make this visible in the deployment guide. |
| R-25 | Backup manifest does not inline remote-tier payload bodies; cross-region L2 replication is external-to-Wendelon | MEDIUM | DR | `ops/backup_dr.md` "Remote-Tier Assumptions" | Document the customer's responsibility explicitly. Provide a reference Terraform module for provider-native cross-region replication. |
| R-26 | `/metrics` on mTLS is excellent but `/health` is unauthenticated by design | LOW | Information leakage | `ops/observability.md` §1, ADR 027 §5 | Acceptable as designed (returns no cluster state). Monitor for label-leakage regressions in future metric additions. |
| R-27 | 60-second signed-root roll-up cadence caps attacker rewrite window at ~1 minute on a single compromised node | LOW | Auditability | ADR 025 §4 | Document the window explicitly in compliance posture. Offer a tighter cadence config for high-assurance tenants (cost trade: event volume). |
| R-28 | No anti-abuse salt for adversarial tenant-controlled locality hotspots | LOW | Multi-tenancy abuse | ADR 029 Open Questions | Defer to W5 tenant-threat-model work. Tracked. |
| R-29 | ADRs are all "Proposed"; none marked "Accepted" | LOW | Process / Procurement | Every ADR reviewed | Close Open Questions and promote to Accepted before GA. Procurement reviewers treat Proposed as unstable. |
| R-30 | `plans/ops/runbooks/` directory is referenced but empty | LOW | Operational readiness | `ops/observability.md` §6 | Populate runbooks for every paging alert (`RaftNoLeader`, `GroupCommitWindowHigh`, `AuditMaterializationLagRising`, `AuditSinkFailing` at minimum). |

#### v2 additions — code-level (Codex) and operational (Gemini)

| # | Finding | Severity | Domain | Evidence | Remediation summary |
|---|---|---|---|---|---|
| R-31 | Shared `io_uring` thread multiplexes Raft WAL durability with slab I/O; does not match the "one ring per physical NVMe device" design claim | **CRITICAL** | Storage hot path | `src/storage/io_uring.rs:3-5`, `src/node.rs:970-996` | Split execution by durability class: one ring/thread for WAL, one per NVMe for slabs. Expose SQ/CQ depth, submit lag, CQE lag as metrics. Move remaining blocking file ops (`src/storage/file_ops.rs:141-305`) onto the correct ring. |
| R-32 | Finalization recovery becomes best-effort when pending-intent persistence fails; quorum-durable data can be stranded without deterministic replay | **CRITICAL** | Stellarator correctness | `src/network/data_server.rs:3402-3447` | Make pending-intent persistence mandatory for the recoverable branch. If persistence fails, fail the write (or fence the node) rather than continuing. This is exactly the partial-failure class a bifurcated model must nail. |
| R-33 | Stellarator write path serializes leader-local durable append before remote fan-out; remote replicas cannot begin durable ack until local fsync returns | HIGH | Write latency / Stellarator claim | `src/network/data_server.rs:3328-3379` | Spawn remote fan-out concurrently with the local durable append; wait on the combined set for quorum. Or explicitly document and justify the serialized design. Current code contradicts the "concurrent fan-out" comment. |
| R-34 | Hot-path request admission clones full `SmState` (epoch_history, leases, tiered_slabs, migrations, tombstones, tenants, tenant_keysets) just to read current epoch | HIGH | Throughput / allocator pressure | `src/network/data_server.rs:3222-3224, 3458-3460`; `src/consensus/raft_storage.rs:1973-1978, 1372-1432` | Replace with a narrow immutable `watch`/atomic cache of the specific admission-required fields. Clone-on-read of the full state machine on every request is O(metadata size) overhead. |
| R-35 | QUIC server has no admission control, per-tenant quotas, per-peer stream caps, or bounded task fan-out; per-connection and per-stream `tokio::spawn` are unbounded | HIGH | Availability / multi-tenant isolation | `src/network/server.rs:48-53, 350-370, 531-538` | Add listener-level admission: max concurrent connections (total and per-peer-IP), max streams per connection, per-tenant backpressure (bounded queues keyed on post-auth `tenant_id`), bounded task pool. Required for shared Fortune-500 infrastructure. |
| R-36 | Transport claims "QUIC on top of `io_uring` + GSO" but the code creates a default `quinn::Endpoint` with no `TransportConfig` tuning, no GSO/GRO handling, no `io_uring` integration | MEDIUM | Truth-in-advertising / performance | `src/network/transport.rs:69-73`, `src/network/quic_transport.rs:179-185` | Either implement the tier-1 transport as documented or retract the claim from the tier description. Benchmark numbers quoted against this tier are otherwise optimistic. |
| R-37 | Framing path copies bytes into pooled `Vec<u8>` buffers under a single `std::sync::Mutex` buffer pool | MEDIUM | Contention at high stream count | `src/network/core.rs:143-160`, `src/network/pool.rs:38-42` | Replace the mutex-guarded free-list with a sharded or lock-free pool (e.g., per-runtime-worker caches with global overflow). Measure and document the zero-copy claim against the actual hot-path code. |
| R-38 | Release builds abort on panic; live paths contain `expect(...)` / poisoned-lock assumptions that can kill the process on rare invariant breaches | HIGH | Blast radius | `Cargo.toml:57-62`, `src/storage/io_uring.rs:339-370`, `src/storage/manager.rs:384-387` | Convert recoverable `expect(...)` sites in live paths to explicit error handling with fencing. Keep abort-on-panic only for truly unreachable invariants that are audited to be unreachable. Document the inventory. |
| R-39 | State machine has acknowledged placeholders: `SchemaUpdate` is not modeled in `SmState`, and several control-ring meta structs are in-process-only with unfrozen wire layouts | HIGH | Wire-compatibility risk at upgrade | `src/consensus/raft_storage.rs:2770-2774`, `src/network/traits.rs:12-19`, `src/client/topology.rs:22-28` | Freeze control-ring wire contracts before any customer-facing commitment. Model schema updates in durable state. Re-run `ops/upgrades.md` rolling-upgrade window against the final wire layout. |
| R-40 | Architecture test (`tests/architecture_test.rs:134-156`) enforces unified-ring guardrail by counting occurrences of the literal word `plane` — does not verify single-membership-graph, single-epoch-authority, or one-listener-topology invariants | LOW | Guardrail strength | `tests/architecture_test.rs:134-156` | Replace the string-count test with structural assertions: one `MembershipConfig` type, one epoch source, no second listener carrying routing traffic. The "forbidden word" check is useful but not sufficient to enforce the philosophy. |
| R-41 | No synchronous node-replacement path; post-eviction cluster runs at degraded durability until external provisioning intervenes (ADR 001 "Level-2 rejected") | HIGH | HA / 99.999% SLO | ADR 001 "autonomous healing boundary"; Gemini §3 | Ship a first-party Kubernetes Operator and/or tightly-coupled sidecar that performs synchronous node replacement without embedding cloud SDKs in the core. Document the replacement latency as part of the RTO/RPO SLA (R-5). |
| R-42 | Lazy migration + migration-read proxy pattern risks thundering-herd when large parallel analytical queries hit a newly reassigned range | HIGH | Multi-tenant stability at topology change | ADR 006, ADR 022-024; Gemini §2 | Add adaptive QoS / backpressure to migration-read proxying. Rate-limit per-source-range and per-proxy-node. Ship the behavior in the chaos harness's topology-change scenarios (ADR 028). |
| R-43 | Kafka audit sink is cargo-feature-gated (`audit-kafka`); default binary cannot ship audit to SIEM without a custom rebuild | MEDIUM | Compliance integration | `ops/audit.md` §7, Gemini §4 | Promote `audit-kafka` to the default build profile. The Kafka client footprint is the stated cost; it is a smaller cost than requiring every regulated operator to maintain a Rust toolchain. Independently, canonicalize the Kafka payload to include chain fields (already R-10). |
| R-44 | No end-to-end Toron (compute-tier) integration runbook; SQL / JDBC / ODBC / SSO / SAML path not operationally validated | HIGH | Enterprise fit | Gemini §4 | Author `ops/toron_integration.md` covering: SQL gateway topology, driver matrices, SSO/SAML binding to the Wendelon mTLS OID model, end-to-end auth flow test, per-tenant quota visibility from SQL clients, observability stitching. |
| R-45 | No documented ELT connector story (Fivetran, Airbyte, dbt) for ingestion | MEDIUM | Ecosystem / ingest | Gemini §4 | Ship reference connectors or document the Arrow-over-QUIC ingestion contract so partners can build them. A "bring-your-own-ingest" SKU is acceptable for v1 if documented. |
| R-46 | Chaos suite is a 3-scenario smoke; simulator ladder has one named scenario; FIPS "sweep" is a binary symbol/string regex, not cryptographic validation | HIGH | Validation depth vs. design claims | `tests/chaos.rs:32-36`, `tests/simulator_ladder.rs:162-164`, `ci/check_fips_symbol_sweep.sh:23-45` | Expand chaos to cover nemesis combinations (partition + disk fill + clock skew), long-horizon schedules, linearizability checks. Expand simulator ladder to the matrix referenced in ADR 028. Keep the FIPS symbol sweep as a regression guard; do **not** describe it as FIPS validation to customers. |
| R-47 | Observability coverage missing: pending-finalization backlog and replay metrics, replica fan-out latency, `io_uring` SQ/CQ depth, WAL fsync latency, active connection/stream counts, lock contention indicators | HIGH | Five-nines operability | Codex "Observability Gaps" | Ship the missing metrics as first-class exports. Metric catalog extension is scoped in `ops/observability.md`; the data is already computable inside the running code. |
| R-48 | Hardcoded policy-values without adaptive control: BufPool shape `(256, 1 MiB)`, `DEFAULT_RING_DEPTH = 1024`, architectural guardrail threshold hardcoded at `2`, chaos seed + 3 scenarios hardcoded | LOW | Capacity adaptability | `src/node.rs:968`, `src/storage/io_uring.rs:27-28`, `tests/architecture_test.rs:136-138`, `tests/chaos.rs:30-36` | Move sizing knobs to config with documented defaults; add capacity-aware control loops where the default is likely wrong on specific hardware. Codex flagged this as a sign that the system carries bench-derived embedded policy. |

---

## 5. Domain-level Findings (Expanded)

### 5.1 High Availability and Disaster Recovery

**Current state.** Raft-based consensus for replicated state (Control Ring WAL + snapshot + vote artifacts); L1 NVMe slabs local per node; L2 object storage for tiered data. `ops/durability.md` documents a 30s-soft / 60s-hard readiness budget per node for recovery within a defined envelope (10 GiB snapshot, 1 GiB WAL tail, 64 GiB local slabs, 8 GiB redb index). The cryptographic posture for slabs is strong (client-side envelope encryption, AEAD with AAD binding). Node lifecycle is well-defined (`ops/lifecycle.md`).

**Gaps.**

- **No cluster-level HA / DR plan.** `ops/backup_dr.md` is 53 lines covering one-node restore; it explicitly defers multi-node cluster replacement choreography, scheduled export, retention management, remote-tier payload validation during restore, and legal-hold policy. For a regulated Fortune-500 deployment, the expectation is a 10-30-page runbook with tested RTO/RPO, cross-region failover decision tree, data-integrity verification post-restore, and legal-hold freeze procedure.
- **No Point-in-Time Recovery (PITR).** Backups are per-node artifact snapshots. Time-travel queries (ADR 013, not reviewed in this pass) may provide some forensic rewind, but PITR for regulatory recovery (e.g., "restore the state of tenant X as of 2026-04-15 14:30 UTC for SEC subpoena response") is not architected.
- **No cross-region replication built in.** L2 tiered objects are the authoritative bytes for cold data; cross-region replication is delegated to the customer's object-store policy (`ops/backup_dr.md` "Remote-Tier Assumptions"). This is operationally reasonable but procurement requires the vendor to ship the reference topology.
- **Durability is not quantified.** `ops/observability.md` §4 explicitly disclaims the "11 nines" figure: *"The pilot does not claim a specific durability figure."* This is honest but will be rejected by any procurement process that includes a contractual durability SLA.
- **SLOs are TARGET (unvalidated).** Per the same document. The chaos+simulator harness (ADR 028) is the validation path; no evidence of soak runs yet.

**Remediation (prioritized).**

1. Author `ops/dr_plan.md` with quantitative RTO (e.g., 4h for warm-standby, 24h for cold-restore) and RPO (e.g., 15m for L1, 1h for L2). Ship a tested failover drill with evidence.
2. Implement cluster-level backup choreography (consistent-cut across all nodes via Raft snapshot coordination + L2 pointer stabilization). Document in `ops/backup_dr.md`.
3. Implement or document a cross-region replication story that is Wendelon-aware: at minimum, a verified-cross-region-replicated L2 bucket convention and a tested bootstrap-from-DR-region procedure.
4. Move SLOs through the TARGET → COMMITTED ladder via the chaos harness before GA.
5. Add backup encryption key management to the backup format (backups themselves contain sensitive material — wrapped Tenant KEKs, replication state, node signing material).

### 5.2 RBAC, Identity, and Separation of Duties

**Current state.** Dual-CA (Node CA + Operator CA) with critical-marked OID extension carrying `{role, scopes, tenant_id, key_id}`. Four roles: `data_node`, `operator`, `read_only_observer`, `break_glass_admin`. Enforcement at a single RPC dispatcher layer. Short-lifetime certs (24h/24h/8h/1h). Revocation split by cert-side (server-side OCSP stapling; client-side short-window CRL pull with 30-minute fail-closed staleness).

**Gaps.**

- **Role coarseness (R-6).** The `operator` role collapses DBA + Platform Engineer + Backup Admin + Tenant Admin. Fortune-500 controls require non-overlapping personae. *Compensation:* scopes are in the OID payload but the ADR does not define scope granularity that would enable sub-operator role construction.
- **No dual-authorization (R-7).** Every destructive RPC (`delete_tenant`, membership change, KEK rotation, break-glass issuance) accepts a single valid signature. The architecture supports dual-auth without change — a Raft proposal could require N-of-M cert signatures — but this is not specified.
- **OID PEN not registered (R-8).** The OID is "TBD at ADR acceptance." Every cert issued before PEN registration is pre-production and must be reissued.
- **Certificate rotation service placement is an Open Question (R-13).** A cert-rotation service that deadlocks against the cluster it certifies is a catastrophic design error. The ADR defers this with *"design note in `plans/network/security.md` before ADR acceptance"* — `plans/network/security.md` is currently 15 lines and does not address this.
- **CRL distribution endpoint is an Open Question (R-14).** The 30-minute fail-closed ceiling is load-bearing but the distribution channel is unresolved.
- **No LDAP / AD / SSO integration.** Operator identity is certificate-only. Fortune-500 typically requires integration with enterprise IdP for operator onboarding/offboarding; cert issuance should key off IdP group membership. Today, issuing an operator cert is an off-path ceremony with no documented tie-back to IdP.
- **Operator CA key protection is unspecified.** ADR 018 §1 says *"Its private key is held off-path (hardware token, HSM, or offline signing workstation) and signs only on demand."* The disjunction is too loose — "offline signing workstation" is weaker than "HSM-backed" by a material margin, and procurement will ask which it is.
- **Break-glass workflow (R-16).** Issuance is an Operator-CA ceremony but no pre-approval workflow, no incident-ticket linkage, no time-boxed scope.
- **No JIT operator elevation.** Cert lifetime is 8 hours; elevation-to-operator is a re-issuance of a new cert rather than a scope-upgrade on an existing session. This is defensible but means every operator session is an 8-hour full-authority window unless operators voluntarily cycle certs more frequently.

**Remediation.**

1. Register the IANA PEN and freeze the OID (hard prerequisite).
2. Split `operator` into the four-role model proposed in §3.2. Keep the dispatcher enforcement layer unchanged.
3. Add dual-authorization to destructive Raft proposals (M-of-N cert signatures).
4. Close the cert-rotation service design (Open Question) with an HA, no-deadlock topology.
5. Close the CRL distribution design (Open Question) with a cluster-state-distributed CRL or a separately-HA endpoint.
6. Mandate HSM-backed Operator CA in the regulated-SKU deployment guide. Do not allow "offline signing workstation" for FIPS/regulated deployments.
7. Document an IdP-integration reference: operator-cert issuance keyed on IdP group membership via a vendor-owned cert-issuance service that validates IdP state.
8. Add a pre-approval workflow for break-glass issuance: second-operator sign-off, incident ticket ID in the parameter field, reason string non-empty and length-bounded.

### 5.3 Encryption Key Lifecycle

**Current state.** Three-tier hierarchy (Root KEK → Tenant KEK → Slab DEK); per-slab DEKs with intra-slab 64 KiB chunked AEAD; bit-exact 48-byte AAD; injective-nonce encoding that survives ADR 019's branch decision. Tenant KEKs in Raft state (wrapped). Zeroize-on-drop, mlock, TTL-bounded caches. Plaintext→ciphertext migration sweeper for legacy slabs. FIPS build profile forces the plain-GCM branch with injective nonces; symbol-sweep CI check verifies SIV is compiled out.

**Gaps.**

- **Production Root KEK provider does not exist (R-1).** `ops/key_rotation.md` plainly states the current provider is `dev-insecure` persisting plaintext at `<data_dir>/.wendelon/dev-root-kek.bin`. This is not an oversight — it is called out as *"a development placeholder, not a cloud KMS / HSM integration."* For production: AWS KMS, GCP KMS, Azure Key Vault, and PKCS#11 HSM providers must be implemented, tested, and documented. The ADR 020 "KMS backend abstraction" Open Question must close.
- **No key escrow (R-2).** Explicit posture: *"Key escrow is out of scope for the pilot. There is no escrow path and no recovery workflow based on escrowed plaintext keys."* And: *"Loss of every valid Root KEK unwrap path is unrecoverable for encrypted data in this pilot."* Regulated Fortune-500 (litigation hold, e-discovery, bankruptcy-trustee data recovery) typically requires an M-of-N custodian-held Shamir split of the Root KEK, sealed in HSMs, ceremoniously managed. Absence of this is a procurement stopper for financial services.
- **No operator surface for rotation (R-11).** `rotate_tenant_kek` exists as an in-process primitive only — no CLI, no admin RPC, no HTTP endpoint. A rotation workflow that requires shelling into a node and running a Rust integration test is not a production workflow.
- **Rotation does not re-wrap existing Slab DEKs (R-12).** New slabs use the new Tenant KEK; historical slabs keep their footer-recorded old `tenant_kek_id` and the old wrapped KEK remains in Raft state indefinitely. This means compromise of a rotated-out Tenant KEK still decrypts historical cold data. NIST SP 800-57 Part 1 rev. 5 §8.2.4 requires re-encryption within the original key's active period or cryptoperiod expiry; Wendelon's current behavior is effectively "never, unless compaction happens to touch the slab."
- **No maximum key age enforcement.** A Tenant KEK can live forever if nothing rotates it. Schedule-driven rotation (e.g., annual for Tenant KEKs, biennial for Root KEKs) is not architected.
- **Plaintext slab buffers are not zeroized (R-15).** `ops/key_rotation.md`: *"Decrypted slab plaintext lives in ordinary process memory and is not zeroized on drop."* For CMVP Security Policy, this is a documented deficiency; procurement will ask "do you zero memory on drop?" and the honest answer today is "not the plaintext, only the keys."
- **FIPS CMVP certificate (R-3, R-24).** ADR 026 is clear that "FIPS-ready" ≠ "FIPS-certified." The CMVP certificate number is the binary procurement artifact. No number recorded. aws-lc-rs upstream CMVP status is an Open Question gating ADR 026 acceptance. Transport TLS is outside the boundary. All three must close before shipping the "FIPS 140-3 certified" SKU.
- **No BYOK (Bring Your Own Key) / HYOK (Hold Your Own Key) story.** Large enterprise customers often require customer-controlled Root KEK (in their KMS, not the vendor's). Not architected.
- **No quarterly key-ceremony documentation.** Regulated customers audit the ceremony log itself.

**Remediation.**

1. Ship AWS KMS + GCP KMS + Azure Key Vault + PKCS#11 HSM providers behind the ADR 020 KMS abstraction. Integration-test each. Close the Open Question.
2. Design M-of-N custodian escrow for the Root KEK. Document the quarterly ceremony. Sell this as a regulated-industry add-on if necessary — the absence is the critical gap.
3. Ship `TenantAdmin.rotate_tenant_kek` with operator-role gating, dual-auth for production, audit emission, and a post-commit CryptoManager-convergence check (the in-process primitive already has this).
4. Add a background DEK-rewrap sweeper that iterates historical slabs under retired Tenant KEK versions, re-wraps their Slab DEKs under the active Tenant KEK, and removes the retired KEK from Raft state when no slab references it. Cap max-time-to-full-rotation as a config-bound SLA.
5. Enforce maximum Tenant KEK age; Raft state-machine rejects writes to a tenant whose active KEK is older than `max_tenant_kek_age` (default 365d).
6. Implement zeroize-on-drop for plaintext slab buffers (coordinate with ADR 009 AlignedBuf).
7. Record the CMVP certificate number in ADR 026 at acceptance. Document the pinned aws-lc-rs version's CMVP status. Land `rustls-aws-lc-rs` for the transport path and expand the FIPS module boundary.
8. Architect BYOK: customer-KMS Root KEK with the Wendelon Tenant KEK wrapped by it. The existing three-tier hierarchy accommodates this with no change to the Slab DEK path.

### 5.4 Auditability

**Current state.** Canonical structured events with a closed operation enum and versioned schema (ADR 025). Per-node hash chain (blake3 default; SHA-256 FIPS). 60s signed-root roll-up. Two authorities (RaftLog / NodeLocal) with honest materialization semantics. Fail-closed on NodeLocal writes. Three sinks (LocalNdjson always-on, Syslog, Kafka). Retention default 90 days, rotation at 1 GiB / 24h.

**Gaps.**

- **No data-access audit (R-4).** Critical gap detailed above. Not in the event enum. Today, an operator-role principal reading tenant X's data generates *no audit record*. HIPAA §164.312(b) and PCI-DSS §10.2.1 are explicit requirements.
- **Sink-shape asymmetry (R-10).** Kafka and Syslog do not carry chain fields. A SIEM cannot independently verify the tamper-evident chain from its Kafka feed; the local NDJSON file is the only compliance-evidence archive. This is documented (`ops/audit.md` §1) but it means the local file must itself be shipped to long-term retention — and the default 90-day local retention is below that need.
- **Retention default is 90 days (R-9).** SOX (7 years), HIPAA (6 years), PCI-DSS (1 year minimum, 3 years typical), banking regulators (5-7 years). Default is two orders of magnitude off for regulated defaults.
- **No WORM / object-lock integration (R-18).** Hash-chain gives tamper evidence but does not prevent an administrator with local root from truncating the file. Procurement often requires both: tamper-evidence *and* tamper-resistance via WORM.
- **Break-glass parameter schema incomplete (R-17).** Open Question in ADR 025. Without it, `break_glass.used` records are structurally under-specified.
- **No trace-id correlation in audit records.** ADR 027 Open Question. Without it, an auditor investigating a suspicious admin action cannot pivot to the trace for the operation.
- **Signed-root key use is an Open Question.** ADR 025 Open Question: whether the node's mTLS signing key doubles as the audit signed-root signing key. If yes, mTLS key compromise compromises the audit chain too.
- **Per-node chain not cluster-wide.** Deliberate and correct for consensus cost, but compliance auditors typically expect a single canonical timeline. The signed roll-up provides the anchor but the roll-up tooling is not documented (no cross-node verifier, no replay/verify CLI).
- **Audit-sink-failure retry and backpressure.** `ops/audit.md` says off-box sink failure does not block the audited operation, and that the failure is audited on the next successful emission. What happens during prolonged local disk pressure is documented only to the level of "admission-control disk-reservation" — the reservation mechanism is not detailed.

**Remediation.**

1. Extend ADR 025 event model with `data.read` and `data.write` event types. Add `sampling_policy` to event schema (e.g., "all writes, 10% reads, 100% of cross-tenant-principal reads"). Measure retention impact.
2. Change default retention to 2555 days (7y). Add a per-tenant retention floor keyed on tenant-compliance-tier config.
3. Canonicalize Kafka payload to include chain fields so off-box SIEM can verify.
4. Implement a WORM/object-lock `AuditSink` implementation for S3 Object Lock (or equivalent). Document chain continuity across rotation boundaries with this sink.
5. Close the break-glass schema Open Question with a concrete parameter list.
6. Close the trace-id Open Question with an optional `trace_id` field on audit records.
7. Close the signed-root-key Open Question — use a separate signing key or document the single-key trade-off explicitly. Favor separate key for regulated SKU.
8. Ship a `wendelon-ctl audit verify` CLI that replays a local chain, cross-correlates against signed roots from peer nodes, and produces a signed attestation suitable for auditor evidence.

### 5.5 Observability

**Current state.** Three-listener posture (`/health` unauth, `/ready` mTLS, `/metrics` mTLS). OTLP-only trace export. OpenMetrics scrape. Cardinality discipline (no per-tenant labels). Default metric catalog covers Raft, transport, storage, L2, crypto, audit, process. Seed alert set with recording rules.

**Gaps (minor, mostly polish).**

- Runbooks empty (R-30). Required for operator readiness.
- FIPS-build metric visibility is concrete for startup self-test but does not expose *ongoing* self-test results (CAVP conditional tests). For a FIPS-certified deployment, conditional-test metric visibility is a Security Policy expectation.
- SLO validation is TARGET-only (R-23).

**Remediation.** Populate runbooks, wire ongoing FIPS conditional-test metrics, run SLO validation harness.

### 5.6 Governance and Operational Readiness

**Current state.** Upgrade strategy is well-specified (`ops/upgrades.md`). Crypto-profile rolling restart is a supported operation with explicit procedure. Ring parameters are immutable post-bootstrap (`ops/ring_management.md`) with a documented cluster-migration escape hatch.

**Gaps.**

- **No incident response plan.** Break-glass cert exists; IR coordination, SIEM alerting playbook, and customer notification procedures are absent.
- **No change management / CAB integration.** Fortune-500 deployments gate all production changes through a change advisory board. Document the change-classification matrix (tenant-affecting, availability-affecting, security-affecting) and the required approvals.
- **No penetration-test evidence.** Required for first-year SOX/PCI deployments. Not addressed.
- **No supply-chain attestation (R-22).** SBOM, signed binaries, SLSA.
- **No deployment-hardening guide.** Operator OS baseline, kernel-version floors (ADR 026 Open Question references this), network-policy minimum, SELinux/AppArmor profiles.
- **ADRs all Proposed, not Accepted (R-29).** Procurement treats Proposed as unstable.

**Remediation.** Author incident response plan, ship supply-chain attestation, produce hardening guide, close Open Questions and promote ADRs to Accepted.

### 5.7 Code-Level Hot-Path Gaps (from Codex's `src/` audit)

v1 of this report was design-level. Codex's peer review exercised the code and surfaced five categories of hot-path gap that change the v2 picture materially. Summarized here; detailed citations in R-31 through R-40 and R-47–R-48.

- **Storage topology divergence (R-31).** The "one `io_uring` per NVMe device" claim is architectural, but the code creates one shared ring per node. Raft WAL and slab I/O contend on one submission queue and one submission thread. This is the single largest hot-path correction on the path to production. Split by durability class: one ring for WAL, one per-NVMe for slabs, with independent queue-depth telemetry.
- **Stellarator is less concurrent than claimed (R-33).** The leader waits for local durable append before spawning remote fan-out. The comment says "concurrent fan-out"; the code says sequential. Every write pays one local fsync barrier before replicas start theirs. At the scale where bifurcated writes are supposed to shine, this is a meaningful tax.
- **Finalization correctness hole (R-32).** When `pending_finalizations.upsert(...)` fails, the path continues and only installs restart-replay bookkeeping on the success branch. A quorum-durable write can be stranded without deterministic replay. This is the exact failure-mode bifurcated writes must nail; leaving it best-effort undercuts the design thesis.
- **Hot-path full-state clone (R-34).** Request admission clones `SmState` — which holds epoch_history, leases, tiered_slabs, migrations, tombstones, tenants, tenant_keysets — to read the current max epoch. O(metadata size) allocator and cache pressure per request. The fix is a narrow `tokio::sync::watch` on the specific admission fields.
- **QUIC hot path is under-tuned and unadmitted (R-35, R-36, R-37).** No admission control, no per-tenant backpressure, no stream caps. Tier-1 transport claims `io_uring` + GSO; the code uses a default `quinn::Endpoint` with no `TransportConfig` tuning. Framing copies through a mutex-guarded global pool. Each of these is tractable; none is architecture-level.

**Remediation priority.** R-31, R-32, R-33 are on the critical path to any customer-facing pilot. R-34, R-35 are required for multi-tenant production. R-36, R-37 are performance truth-in-advertising and should close before benchmarks are published externally.

### 5.8 Day-2 Operations and Ecosystem Integration (from Gemini)

Gemini's lens is operational and ecosystem. The core observations:

- **Level-2 healing gap is a 99.999% blocker (R-41).** ADR 001 explicitly rejects infrastructure provisioning integration. Wendelon evicts a failed node correctly and quickly; what it cannot do is replace it. Until external provisioning intervenes, the cluster runs at degraded replication factor. Gemini's recommendation — a first-party K8s Operator or sidecar that performs synchronous replacement — keeps the core free of cloud SDKs while closing the durability window. This is consistent with the ADR's "autonomous healing boundary" framing and should be committed pre-pilot.
- **Toron compute-tier integration is undefined (R-44).** Wendelon's enterprise value proposition includes SQL warehousing; SQL, JDBC/ODBC, SSO/SAML all live in Toron. The end-to-end operational boundary between storage and compute — how an operator on one side sees tenant quota / audit / identity from the other, how mTLS OID maps to SQL session authorization — has no runbook. A storage-only pilot is not an enterprise pilot.
- **Thundering-herd on lazy migration (R-42).** The lazy-migration + migration-read-proxy pattern is correct for avoiding rebalance storms, but at the moment of topology change, a large analytical query hitting a newly-reassigned range turns the proxy node into a bottleneck. Per-tenant QoS and per-source-range rate-limiting on migration reads are the mitigation; they should be exercised in chaos scenarios before GA.
- **Kafka audit sink is behind a cargo feature flag (R-43).** Standard binary cannot ship audit to SIEM without a custom rebuild. This turns every regulated operator into a Rust toolchain maintainer. Promote to default, accept the client footprint cost.
- **ELT connector story is absent (R-45).** Fortune-500 warehouses ingest from Fivetran/Airbyte/dbt pipelines. Wendelon's Arrow-over-QUIC ingestion contract is defensible as the interface, but partner connectors do not exist and are not documented.

**Remediation priority.** R-41, R-44 gate regulated pilot. R-42 is a chaos-scenario requirement. R-43, R-45 are fast and should land before first customer conversation.

---

## 6. Remediation Roadmap (Suggested Phasing)

**Gate 1 — Must close before any regulated pilot deployment (blockers):**
R-1 (production KMS), R-2 (key escrow), R-3 (FIPS transport), R-4 (data-access audit), R-5 (DR plan with RTO/RPO), R-8 (OID PEN registered), R-13 (cert-rotation service), R-14 (CRL distribution), R-17 (break-glass schema), **R-31 (io_uring topology split), R-32 (mandatory pending-intent persistence), R-41 (synchronous node replacement), R-44 (Toron integration runbook)**.

**Gate 2 — Must close before GA in regulated industries:**
R-6 (role split), R-7 (dual-auth), R-9 (retention default), R-10 (Kafka canonical shape), R-11 (rotation operator surface), R-12 (DEK re-wrap sweeper), R-15 (plaintext zeroize), R-16 (break-glass workflow), R-18 (WORM sink), R-19 (GDPR right-to-erasure), **R-33 (Stellarator concurrent fan-out), R-34 (SmState hot-path clone removed), R-35 (QUIC admission control), R-39 (control-ring wire freeze), R-42 (lazy-migration QoS), R-43 (Kafka sink default), R-46 (chaos+simulator coverage), R-47 (observability coverage)**.

**Gate 3 — Track for GA quality bar:**
R-20 (quota overshoot documented in SLA), R-21 (Phases 1–2 scheduled), R-22 (SBOM/SLSA), R-23 (SLO validation), R-25 (DR Terraform reference), R-29 (ADRs Accepted), R-30 (runbooks populated), **R-36 (transport claim truth-up), R-37 (lock-free buffer pool), R-38 (expect/abort inventory), R-40 (architecture-test strengthening), R-45 (ELT connectors), R-48 (policy-value configurability)**.

**Gate 4 — Post-GA / staged:**
R-24 (mixed-profile posture), R-26 (observability polish), R-27 (tighter roll-up cadence option), R-28 (anti-abuse salt).

---

## 7. Unified Ring Conflict Summary

A succinct table for the procurement conversation:

| Enterprise expectation | Wendelon position | Conflict? | Resolution path |
|---|---|---|---|
| Separate control plane from data plane at network layer | One unified ring; role-split at OID | Perceived, not architectural | Document AC-4/SC-7 mapping; offer admin-listener-on-separate-VLAN deployment mode |
| Four+ distinct privileged roles (DBA / Sec / Audit / Ops) | Four roles, `operator` very broad | **Real** | Split operator into `tenant_admin`, `cluster_admin`, `backup_admin`, `compliance_auditor` (R-6) |
| Physical tenant isolation (CPU/disk/net/cache) | Logical only, Phases 1–3 post-pilot | **Real** | Schedule Phases 1–2 into pre-GA for regulated SKU (R-21) |
| Data residency enforced at storage layer | One cluster L2 bucket default | **Real** | Phase-2 per-tenant L2 bucket required for EU/APAC residency (R-21) |
| Dual-control on destructive ops | Single signature | **Real** | M-of-N in Raft proposal validation (R-7) |
| Data-access audit trail | Admin-events only | **Real** | Extend ADR 025 event enum (R-4) |
| Key escrow for legal-hold / e-discovery | No escrow | **Real** | Design M-of-N Shamir escrow (R-2) |
| Tamper-resistant audit archive (WORM) | Tamper-evident (hash chain) only | **Real** | Ship WORM sink (R-18) |
| CMVP-validated crypto end-to-end | At-rest only; transport outside | **Real** | Land rustls-aws-lc-rs; expand module boundary (R-3) |
| Tested RTO/RPO | SLOs are TARGET (unvalidated) | **Real** | Chaos harness validation; DR drill evidence (R-5, R-23) |
| Synchronous node replacement after failure | Level-1 eviction only; Level-2 rejected | **Real** | Ship K8s Operator / sidecar (R-41) |
| Capability-filter overhead at thousands of vnodes | Not benchmarked | Potential | Benchmark; cache per-epoch capability sets if measurable |
| Hash-parameter immutability → cluster-clone escape hatch | Documented | Acceptable with pricing | Honest SLA pricing; over-provision projection headroom now |

---

## 8. Closing

v2 update: three independent review lenses (Claude design-level, Codex code-level, Gemini operational) converged on a regulated-pilot readiness band of ~48–55%. That agreement is itself a finding — the system is not near-ready along *any* of the three axes, but it is also not broken along any of them. The work to close is bounded and well-scoped; none of the peer findings demand re-architecture.

Wendelon's architecture is strong in the technical dimensions that are hardest to retrofit — unified-ring correctness, Identity/Locality types, envelope encryption hierarchy, tenant-aware schema, tamper-evident audit model. The remediation work is overwhelmingly in the *integration, operational, and governance* layers around that architecture, not in the architecture itself. This is the right shape of gap: when the architecture is sound, operational debt is addressable on a calendar. The inverse would be much worse.

The five CRITICAL gaps (R-1 through R-5) are all closable with bounded engineering effort and clear scope. Until they close, Wendelon is not deployable in a regulated Fortune-500 setting regardless of the strength of the underlying design. With them closed and Gate 2 addressed, Wendelon is competitive with established incumbents in the regulated-warehouse space and significantly ahead of them on a few axes (day-one tenant-aware schema, two-authority audit model, Identity/Locality split).

The "Unified Ring" philosophy is not a blocker for enterprise adoption, but it requires documentation discipline that the current plans/ tree does not yet carry. Procurement reviewers will not accept philosophy as a control objective; they will accept philosophy *mapped to* control objectives. That mapping is inexpensive to produce and should be authored before the first regulated pilot conversation.
