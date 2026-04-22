# Workstream B — Crypto Custody and Key Lifecycle

**Merged-plan anchor:** WS4
**Author / Role:** Claude, as Lead Architect on Workstream B
**Version:** v2 — integrates peer review of `codex_workstream_E_20260422.md`. Codex and Claude broadly agree on the problem scope; we disagree on one load-bearing decision (one-provider-first vs. four-providers-at-once). The disagreement is documented in §9, not resolved; it should be a Design-Lock entry.
**Phase alignment:** Design Phase 0 (after ADR 020 Accepted, Week 3); implementation Phases 1-2; hardening Phase 3.
**Gap items closed:** R-1, R-2, R-11, R-12, R-15.
**Depends on:** WS-A (ADR 020 acceptance, KMS vendor contracts). Feeds: WS-D (Tenant-KEK audit events), WS-G (backup encryption, via Codex/Gemini's WS7), WS-E (FIPS module boundary).

---

## 1. Why this workstream exists

Today the only Root-KEK provider in the code is `DevInsecure`, which persists the Root KEK in plaintext at `<data_dir>/.wendelon/dev-root-kek.bin`. `ops/key_rotation.md` explicitly says this is not a production custody boundary. Loss of every valid Root KEK unwrap path is documented as *unrecoverable*. For a regulated Fortune-500 buyer, this single fact is a procurement stopper regardless of how strong the rest of the architecture is.

This workstream closes the distance between the well-designed three-tier key hierarchy in ADR 020 (Root KEK → Tenant KEK → Slab DEK) and an actual production-ready implementation of that hierarchy. It also closes the gap between the zeroize-on-drop discipline we apply to keys and the plaintext slab buffers we do *not* currently zeroize.

## 2. Strategic position (opinionated)

Five architectural positions. Each has been contested in prior discussions and each is load-bearing.

### 2.1 Four providers shipped in Phase 1, not one — but with a defensible escape hatch

The counter-argument, articulated clearly in Codex's WS-E, is *"do not build a giant KMS abstraction before one provider actually works end to end."* This is a legitimate software-engineering caution: shipping four partially-working providers is worse than shipping one that works well. Codex proposes AWS first, GCP/Azure "quickly behind it."

I hold the four-at-once position for Phase 1 exit, but I concede Codex's point on the risk surface and add a defensible fallback:

**Primary plan:** AWS, GCP, Azure, PKCS#11 all ship by Phase 1 exit (Month 10). Trait deliberately kept narrow (`wrap`, `unwrap`, `rotate`, `retire_key`, `health_check`) to prevent the "giant abstraction" failure mode.

**Fallback if engineering velocity constrains:** AWS KMS ships first (Phase 1 Month 2-3 production-quality, required for ending `DevInsecure` default). GCP and Azure ship Phase 2 Month 10-12. PKCS#11 ships Phase 2 Month 13-14. First regulated pilot can deploy against AWS KMS only if the customer's cloud is AWS.

**Why I still prefer the primary plan over the fallback.** The procurement cost of "FIPS-compliant SKU on AWS only" is lost RFPs; the engineering cost of three additional thin providers is ~1500 lines against a narrow stable trait. The trade favors parallel shipping. But I acknowledge Codex's concern: if the AWS integration itself reveals trait design issues that require breaking changes, landing the other three late — with the trait stable — is the right order.

**Decision requirement.** This disagreement should be a Design-Lock entry (DL-Bxxx) before Phase 1 Week 1. Either the primary plan (my bias) or the fallback (Codex's bias) ships; the program cannot run both.

**Rationale (unchanged).** The KMS API surface is narrow: `wrap(key_id, plaintext) -> wrapped_bytes`, `unwrap(key_id, wrapped_bytes) -> plaintext`, `rotate(key_id) -> new_key_id`. Each provider is roughly 300-500 lines of Rust plus its SDK. Four providers is ~2000 lines total. Against that, the procurement cost of saying "FIPS-compliant SKU on AWS only" is losing every non-AWS customer at RFP stage.

### 2.2 Shamir 3-of-5, HSM-sealed shares, quarterly recombine

Escrow is mandated. The question is shape. I argue 3-of-5 Shamir Secret Sharing with shares sealed in custodian HSMs. Rationale:

- **3-of-5 vs 2-of-3.** Five custodians tolerates two unavailable (vacation, incapacity, departure) without triggering an emergency ceremony. Three shares to recombine still fits a normal ceremony room. 2-of-3 is fragile under real organizational turnover.
- **HSM-sealed shares, not laptop files.** Each custodian holds a PKCS#11-sealed share on their own HSM. Recombination happens in a secure ceremony room where custodians physically bring their HSMs, shares are unsealed into a ceremony HSM, Shamir recombination produces the Root KEK, which is immediately used and never persisted in plaintext.
- **Quarterly recombine-verify ceremony.** Every 90 days: unseal all shares, recombine, verify the recombined Root KEK can unwrap a canary Tenant KEK, re-split and re-seal. This exercises the ceremony; ceremonies nobody has run do not work when you need them.
- **Annual full key-rotation ceremony.** Every 365 days: recombine, rotate the Root KEK (via the KMS provider), re-wrap every Tenant KEK, destroy the old Root KEK, re-Shamir-split and re-seal.

Opposition position: "escrow is for customers to do, not the vendor." Rejected. The vendor ships the *mechanism* and the *ceremony documentation*; the customer operates it. Shipping a key hierarchy with a documented "there is no recovery if you lose the KMS" posture is not acceptable procurement.

### 2.3 Tenant-KEK rotation re-wraps existing Slab DEKs; it does not rely on compaction

The current code lets old Tenant KEK versions live in Raft state indefinitely, re-wrapping Slab DEKs only when compaction happens to touch the slab. Cold slabs therefore keep their original Tenant-KEK wrap forever. This violates NIST SP 800-57 Part 1 Rev 5 §8.2.4 (re-encryption within the original key's cryptoperiod).

I argue for a **background DEK re-wrap sweeper** that runs independently of compaction, iterates slabs under retired Tenant-KEK versions, re-wraps their Slab DEKs under the active Tenant KEK, and marks the retired KEK for destruction when its refcount reaches zero. Configurable rate-limit; default max-time-to-full-rotation SLA 30 days; hard limit 90 days.

**The hard limit is load-bearing.** If the sweeper cannot complete within 90 days, the cluster is rejecting its own cryptoperiod compliance; an alert pages the operator. This is deliberately aggressive — a pilot customer who is told "you might be in key-rotation debt for months" is not going to ship.

### 2.4 Tenant KEK has a maximum age enforced at state-machine apply

Even without compromise-driven rotation, every Tenant KEK has a maximum age. The state machine rejects *writes* (not reads) to a tenant whose active Tenant-KEK age exceeds `max_tenant_kek_age` (default 365 days, configurable per tenant). An operator who has not rotated in a year must either rotate or explicitly extend the age limit for that tenant.

This is a forcing function for operational hygiene. Without it, a cluster running for two years with no rotation is a pass — and then it's a fail at audit.

### 2.5 Plaintext slab buffers are zeroized on drop; no exceptions

The current code zeroizes KEKs and DEKs but leaves decrypted slab plaintext in ordinary `Vec<u8>` memory. This is a defensible choice for pilot if you're honest about it, and `ops/key_rotation.md` is honest: *"Decrypted slab plaintext lives in ordinary process memory and is not zeroized on drop."* For a regulated SKU, this is a Security Policy deficiency.

I argue for wrapping `AlignedBuf` (ADR 009) in a `ZeroizeOnDrop` equivalent. The subtle piece: `munmap` zeroing is OS-dependent and not always performed; we must zero before unmap. This affects the io_uring path (WS2/WS-codex) — the buffer lifecycle between slab-manager, io_uring worker, and AEAD decrypt layer must coordinate zeroization timing. Architect-level constraint: every code path that holds a plaintext slab buffer must either zero-on-drop the buffer itself or prove the buffer is never reachable again.

**Opposition:** "zeroizing in userspace is theater; memory is still visible to root." Partial answer: CMVP Security Policies ask the question; we give the honest answer. Full answer: zeroization eliminates the dump-core-and-grep attack, which is real even when root-level compromise is the stronger threat.

## 3. Design — the components

### 3.1 `RootKekProvider` trait

```rust
// src/crypto/providers/mod.rs
#[async_trait]
pub trait RootKekProvider: Send + Sync + 'static {
    /// Wrap plaintext key material under the Root KEK identified by key_id.
    async fn wrap(&self, key_id: &RootKekId, plaintext: &[u8]) -> Result<Vec<u8>, KmsError>;

    /// Unwrap wrapped key material under the Root KEK identified by key_id.
    async fn unwrap(&self, key_id: &RootKekId, wrapped: &[u8]) -> Result<Zeroizing<Vec<u8>>, KmsError>;

    /// Rotate the Root KEK; produces a new key_id; the old key_id remains usable for unwrap
    /// until the operator explicitly retires it via `retire_key`.
    async fn rotate(&self, current_key_id: &RootKekId) -> Result<RootKekId, KmsError>;

    /// Retire an old key_id. Subsequent unwrap calls with this id return KmsError::KeyRetired.
    async fn retire_key(&self, key_id: &RootKekId) -> Result<(), KmsError>;

    /// Health check; used at node startup and on a periodic basis.
    async fn health_check(&self) -> Result<(), KmsError>;

    /// Provider identity for metrics and audit.
    fn provider_name(&self) -> &'static str;
}
```

Design notes:

- `unwrap` returns `Zeroizing<Vec<u8>>` so the plaintext zeroes on drop even if the caller forgets.
- `rotate` produces a new key_id rather than mutating in place; old key_id remains for unwrap until explicit retirement. This supports the DEK re-wrap sweeper in §3.4.
- `retire_key` is separate from rotation. A customer may want to rotate keys weekly for defense-in-depth without ever retiring old keys (for legal-hold purposes).

### 3.2 Provider implementations

**AWS KMS provider.**
- Uses `aws-sdk-kms` crate.
- IAM role-based authentication (no stored credentials).
- `key_id` is the AWS KMS key ARN.
- `wrap` uses `Encrypt` API, `unwrap` uses `Decrypt`.
- Grants required: `kms:Encrypt`, `kms:Decrypt`, `kms:GenerateDataKey`, `kms:DescribeKey`, `kms:CreateKey` (for rotate), `kms:ScheduleKeyDeletion` (for retire).

**GCP Cloud KMS provider.**
- Uses `google-cloud-kms` or equivalent.
- Service-account-based authentication.
- `key_id` is `projects/P/locations/L/keyRings/R/cryptoKeys/K/cryptoKeyVersions/V`.
- KMS-side automatic rotation is *disabled*; Wendelon owns rotation cadence.

**Azure Key Vault provider.**
- Uses `azure_security_keyvault`.
- Managed-identity authentication.
- `key_id` is the Key Vault key URI.
- Soft-delete is *enabled* on the key vault; `retire_key` uses soft-delete (allows recovery during 90-day retention window).

**PKCS#11 HSM provider.**
- Uses `cryptoki` crate.
- Token URI and PIN/keyID configured.
- `key_id` is the CKA_ID of the key inside the HSM.
- `wrap` uses CKM_RSA_PKCS_OAEP or CKM_AES_KEY_WRAP; vendor-specific mechanism list documented per supported HSM.
- Tested against SoftHSM2 (CI) and at least one physical HSM (Thales Luna or Entrust nShield, via a dedicated lab) before ship.

### 3.3 Shamir escrow design

**Library.** `vsss-rs` (Verifiable Secret Sharing Schemes, Rust) or `sharks` — architect decision at ADR 020 acceptance. Requirement: constant-time share computation; audited implementation.

**Share format.**
```
Share {
    version: u8 = 1,
    threshold: u8 = 3,
    total_shares: u8 = 5,
    share_index: u8,  // 1..=5
    share_value: Zeroizing<Vec<u8>>,
    share_commitment: [u8; 32],  // BLAKE3 of share_value for integrity check
    wrapped_under_custodian_hsm: bool,
}
```

**Ceremony flow:** documented in `plans/ops/key_ceremony.md` (a new document, shipped as part of this workstream). Ceremony specifies: attendance (at least 3 custodians physically present for recombine), recording (every step logged, audit-evidence-grade), HSM setup (ceremony HSM freshly initialized per ceremony), verification (recombined Root KEK unwraps a canary Tenant KEK before ceremony closes).

**Opinionated position:** the first ceremony, Phase 1 Week 4, is a **dry run**. The first real ceremony that produces the production Root KEK shares is Phase 1 Week 6, after the dry run has exposed any procedure issues.

**Ceremony versioning and audit (added from Codex's open question on repeatability).** The ceremony protocol is a versioned artifact: `plans/ops/key_ceremony.md` v1 is the initial version, v2 updates are governed by DL entries, every executed ceremony records the protocol version it followed. Each ceremony produces a signed attestation (ceremony log + custodian signatures + post-ceremony verification output); the attestation is chained into the audit log via a `ceremony.executed` event (coordinates with WS-D). This ensures ceremonies are repeatable by protocol version, not by "whoever ran the last one remembers." The attestation becomes part of the compliance-pack evidence (WS-E §3.7) for regulated customers.

### 3.4 DEK re-wrap sweeper

A low-priority background task per node that:

1. Iterates slabs whose footer-recorded `tenant_kek_id` is not the tenant's active Tenant KEK version.
2. Unwraps the Slab DEK using the *retired* Tenant KEK.
3. Re-wraps under the *active* Tenant KEK.
4. Updates the slab footer (ADR 022 footer edit) with the new `tenant_kek_id` and `wrapped_slab_dek`.
5. Decrements the retired Tenant KEK's refcount in Raft state.
6. When a retired Tenant KEK's refcount reaches zero, emits `crypto.tenant_kek_retired` audit event and removes the wrapped KEK from Raft state.

**Rate limiting.** `max_rewrite_bytes_per_sec` per node; default 50 MiB/s (low enough not to compete with tiering traffic, high enough to finish a typical tenant within 30 days).

**SLA enforcement.** A metric `wendelon_crypto_rewrap_age_hours` tracks the oldest unrewrapped slab under a retired KEK. Alert if it exceeds 30 days; page if 90 days.

**Edge case.** A tenant in `suspended` state: the sweeper still runs for that tenant's slabs (we cannot read tenant data, but we can re-wrap at the KEK layer — the wrapped Slab DEK is unwrapped using the retired KEK and re-wrapped using the active, never exposing plaintext data). This is important: suspending a tenant cannot indefinitely prevent KEK retirement.

### 3.5 `TenantAdmin.rotate_tenant_kek` admin RPC

Replaces the in-process `WendelonNode::rotate_tenant_kek` primitive with a proper admin RPC.

- **Role:** requires `tenant_admin` (new role from WS-C) or `backup_admin`.
- **Dual-authorization:** required. Two distinct cert key_ids sign the Raft proposal (WS-C §3.4).
- **Audit:** emits `crypto.tenant_kek_rotated` with parameters `{target_tenant_id, old_version, new_version, both_signers}`.
- **Post-commit validation:** the RPC does not return success until the local `CryptoManager` has converged to the new version (this check already exists in the in-process primitive; preserved).
- **Idempotency:** repeated invocation within a retry window returns the already-committed state, not a new rotation.

### 3.6 Plaintext buffer zeroization

New type: `ZeroizingAlignedBuf` wraps `AlignedBuf` (ADR 009) with `ZeroizeOnDrop`. Every code path that allocates a slab plaintext buffer uses `ZeroizingAlignedBuf`.

Coordination with WS2 (Codex-owned):

- `io_uring` submission takes a `ZeroizingAlignedBuf`; the io_uring worker's CQE handler returns ownership to the AEAD layer, which zeroes on drop.
- `munmap`-based buffers in the hot read path explicitly zero their mapped region before unmap. This is non-trivial on Linux (write zeros to the mmap region, then munmap); the zeroization cost on a hot read is ~1 ms per 64 MiB, which is acceptable against a cold-read's L2 fetch latency.

## 4. Work plan (Phase-aligned)

### Phase 0 (Weeks 3-6, after ADR 020 Accepted)

| Week | Deliverable |
|---|---|
| 3 | `RootKekProvider` trait merged. Dev-insecure retained behind `--features dev-insecure`; default build does not link it. |
| 3 | Ceremony doc `plans/ops/key_ceremony.md` drafted. |
| 4 | Escrow library selected (`vsss-rs` or `sharks`); evaluation doc added to Design-Lock log. |
| 4 | KMS vendor sandbox keys provisioned (AWS, GCP, Azure). |
| 5 | DEK re-wrap sweeper design doc. |
| 6 | Phase 0 exit: AWS KMS provider at alpha quality (wrap/unwrap work against sandbox). |

### Phase 1 (Months 2-10)

| Month | Deliverable |
|---|---|
| 2 | AWS KMS provider at production quality. GCP Cloud KMS in development. |
| 3 | GCP + Azure providers at production quality. PKCS#11 provider against SoftHSM2. |
| 4 | PKCS#11 provider against physical HSM (Thales Luna lab). Escrow dry-run ceremony. |
| 4 | `TenantAdmin.rotate_tenant_kek` admin RPC lands. Dual-auth integration pending WS-C. |
| 5 | DEK re-wrap sweeper lands. Rate-limit, SLA metrics. |
| 5 | Production escrow ceremony executed; Shamir shares distributed to custodians. |
| 6 | Plaintext buffer zeroization lands (coordinated with WS2 hot-path refactors). |
| 7 | Max Tenant-KEK age enforcement lands. Per-tenant max-age config. |
| 8 | BYOK surface lands (customer-KMS Root KEK with Wendelon Tenant KEK wrapped by it). |
| 9 | Quarterly ceremony dry-run (second ceremony). |
| 10 | Phase 1 exit: all providers shipped, escrow operational, rewrap sweeper measured, `DevInsecure` removed from default build. |

### Phase 2+ (Months 10-15)

Hardening, soak testing, FIPS integration with WS-E, regression coverage in the chaos harness.

## 5. Interfaces with other workstreams

- **WS-A (Governance):** blocks on ADR 020 acceptance (Week 3) and KMS vendor contracts (Week 4).
- **WS-C (Identity):** provides dual-auth mechanism (Raft-proposal cosigning). `rotate_tenant_kek` waits on this.
- **WS-D (Audit):** emits `crypto.tenant_kek_rotated`, `crypto.tenant_kek_retired`, `crypto.startup`, `crypto.decrypt_rejected` events. Schema coordinated with WS-D.
- **WS-E (FIPS):** FIPS build profile compiles out the `DevInsecure` path unconditionally. Provider implementations respect the FIPS build's cipher-selection constraints (ADR 026 §2a).
- **WS1/2 (Codex-owned runtime):** `ZeroizingAlignedBuf` coordination; io_uring buffer lifecycle.
- **WS7 (DR):** backup encryption uses the KMS providers; wrapped-Tenant-KEKs in the backup manifest. Gemini's WS-A raises a companion principle worth echoing here: *"keep backups logically and physically separate from the L2 data tier to prevent a single bucket corruption from destroying both live data and backups."* The backup-encryption design in WS-B respects this: the Backup DEK is wrapped by the Root KEK (not the Tenant KEK), and the backup destination bucket is a separate object store from the L2 payload bucket by default. This is a DL entry candidate (DL-Bxxx: "backup storage is physically separate from L2 bucket by default").

## 6. Exit criteria

1. Default build contains no `DevInsecure` provider; CI enforces.
2. All four providers (AWS, GCP, Azure, PKCS#11) pass integration tests against live sandboxes / HSMs.
3. Escrow dry-run and production ceremonies both documented and signed.
4. `TenantAdmin.rotate_tenant_kek` admin RPC with dual-auth lands; integration test exercises rotation + post-commit convergence check.
5. DEK re-wrap sweeper measured against a test fleet; `wendelon_crypto_rewrap_age_hours` below 30d threshold.
6. Plaintext buffer zeroization lands; CI enforces `ZeroizingAlignedBuf` for every slab-plaintext allocation path.
7. Max Tenant-KEK age enforced at state machine apply; integration test exercises "write rejected when tenant-kek-age exceeds max".
8. BYOK surface lands with at least one customer-KMS pattern documented end-to-end.
9. `ops/key_rotation.md` retires the "no-escrow posture" section.
10. Scorecard row "Compliance / FIPS evidence" green for at-rest module boundary.

## 7. Risks within this workstream

- **HSM procurement lead time.** Physical HSM for the PKCS#11 provider may have 8+ week lead time. Mitigation: SoftHSM2 for CI from Week 4; physical HSM lab dependency on WS-A Phase 0 procurement; fallback to "PKCS#11 validated against SoftHSM2 only" for Phase 1 exit if hardware slips, flagged to customers.
- **Ceremony procedure error.** Custodians may lose or misuse share HSMs. Mitigation: dry-run ceremony before production; annual re-training; published runbook.
- **DEK re-wrap sweeper competes with tiering traffic.** Mitigation: configurable rate-limit; observable; can be paused during peak.
- **Zeroization introduces regressions in the io_uring path.** Mitigation: behind a feature flag initially; chaos harness exercises before default.
- **KMS API rate-limit encountered at fleet scale.** Mitigation: batch-by-tenant ingress (already in ADR 021 §6); per-provider rate-limit documentation; operator alerts on throttling.
- **Escrow library audit status.** `vsss-rs` and `sharks` both claim constant-time but neither has a recent formal audit. Mitigation: architect review of the source; consider commissioning an audit as part of WS-E Security Policy scope.

## 8. Points of agreement and divergence with Codex's WS-E

**Agreement:**
- `dev-insecure` is development scaffolding; impossible to load in default production profiles.
- Tenant KEK rotation RPC is minimum-production scope, not post-pilot.
- Plaintext zeroization is a real requirement, not compliance decoration.
- DEK re-wrap sweeper is required (not optional) to converge old slabs onto active KEK.

**Refinement from Codex:**
- "Do not build a giant KMS abstraction before one provider works end to end." Adopted as a constraint on the trait design (§3.1): trait is narrow, 5 methods, stable before the second provider lands.
- "How do we version and audit escrow ceremonies?" Answered in §3.3 — versioned protocol, signed attestations, `ceremony.executed` audit event.

**Divergence (unresolved, needs DL entry):**
- **One provider first vs four at once.** Codex argues AWS first, then GCP/Azure "quickly behind." I argue four at once. My fallback plan in §2.1 preserves both positions; the program must pick. This is the single largest decision in WS-B and both positions are defensible.

**Opinionated Constraints (adopted framing from Codex's WS-E):**
- Do not build a giant KMS abstraction before one provider actually works end to end.
- Do not accept "dev only in practice" as a substitute for hard production gating.
- Do not ship rotation without a convergence story for old DEK wraps.
- (Added by Claude:) Do not leave plaintext buffers unzeroized; plaintext hygiene is a CMVP evidence item, not a nice-to-have.

## 9. Open questions

1. **Shamir library selection.** `vsss-rs` vs `sharks` vs `threshold-secret-sharing` — each has trade-offs. Audit status, API ergonomics, dependency weight, constant-time guarantees. I lean `vsss-rs` for its verifiable-commitment support but want the security architect's read.

2. **Custodian selection for escrow.** Five custodians across what organizational scope? All Wendelon employees? Board-level? Mixed (two executive, two engineering, one external trustee)? This is a governance decision that the architect role should propose but not unilaterally decide. I propose: 2 executive, 2 engineering director/architect, 1 external trustee (e.g., a legal advisor).

3. **Is BYOK required for Phase 1 or can it defer to Phase 2?** BYOK adds customer-trust surface. I proposed Month 8 (late Phase 1). Argument for Phase 2: fewer things at Phase 1 exit. Argument for Phase 1: first regulated customer likely asks for it.

4. **Physical HSM vendor for lab testing.** Thales Luna vs Entrust nShield vs YubiHSM 2 (low-end). Procurement-driven choice, but architect cares: PKCS#11 mechanism coverage varies, and Security Policy quality depends on us testing against one that the CMVP lab has familiarity with.

5. **Cryptoperiod for Tenant KEKs.** NIST SP 800-57 Part 1 Rev 5 Table 1 suggests 2 years for symmetric KEK cryptoperiod. I proposed 365-day default `max_tenant_kek_age`. Is 2 years the better default for regulated? Or is 1 year a forcing function worth paying for? I lean 1 year as default with per-tenant override to 2 years allowed.

6. **Is `retire_key` a separate operation from `rotate`, or are they combined?** I proposed separate. The case for combined: operators will almost always rotate-and-retire together, and two-step operations are error-prone. The case for separate: legal-hold scenarios need rotation without retirement. I lean separate; looking for counter-argument.

7. **Per-tenant Root KEK (Phase 3 of ADR 021) — when?** The ADR defers to post-pilot. But a financial-services customer with extreme isolation needs may ask at regulated pilot. Do we commit a Phase-2 schedule, defer to post-GA, or ship as a regulated-SKU add-on?

8. **DEK re-wrap on plaintext slabs still existing.** The plaintext-to-ciphertext migration sweeper (ADR 020 §2.7) and the DEK re-wrap sweeper are architecturally similar but logically separate. Should they share code, or stay separate for clarity? I lean share; want feedback from the WS-codex side whose `storage sweeper` this sits next to.

9. **Memory-dump attack surface.** Plaintext buffer zeroization addresses one attack surface. The other is the `mlock`-protected KEK cache pages: if an attacker has process-level access they can still read `mlock`-resident memory. Do we add `mprotect` + lazy-unlock, or accept as out-of-scope for Level-1 CMVP?

10. **BYOK cross-KMS replication for DR.** If a customer brings their own KMS, the cross-region DR path requires either (a) customer-KMS in both regions, or (b) Wendelon managing cross-region KEK replication. Who owns which? This crosses into WS7 territory — worth flagging here.

11. **Primary-plan vs fallback-plan for provider shipping (new, from Codex disagreement).** Four-at-once vs AWS-first. I've put both in §2.1 with the primary being my position; the program must Design-Lock one. This is the single most contested decision in WS-B and it affects Phase 1 exit content materially.

12. **Evidence criterion for "plaintext buffers adequately zeroized" (adopted from Codex's open question).** What concrete evidence satisfies the CMVP Security Policy? I propose: (a) static analysis pass asserting every slab-plaintext allocation path goes through `ZeroizingAlignedBuf`, (b) runtime test that dumps process memory after a drop and greps for known-plaintext-patterns, (c) io_uring CQE-handler test verifying buffer zero after munmap. Is this enough, or does the lab demand a formal specification?

These should be answered by Design-Lock entries or ADR 020 addenda before Phase 1 Week 4 (when the production escrow ceremony executes).
