# Workstream E — FIPS 140-3, Supply Chain, and Release Compliance

**Merged-plan anchor:** WS9
**Author / Role:** Claude, as Lead Architect on Workstream E
**Version:** v2 — integrates peer review of `codex_workstream_J_20260422.md`. Codex and Claude are near-perfectly aligned on this workstream; the peer review surfaces refinements on pilot-language and fallback posture rather than disagreements.
**Phase alignment:** Phase 0 lab engagement; Phase 1 transport migration and SBOM/signing; Phase 2 CMVP submission and certification.
**Gap items closed:** R-3, R-22, R-24, R-26, R-29.
**Depends on:** WS-A (ADR 026 acceptance, CMVP lab contract, ADR acceptance cadence). Interlocks with: WS-B (crypto primitives routed through FIPS module), WS-D (audit hash primitive switches under FIPS), WS-C (identity signatures under FIPS).

---

## 1. Why this workstream exists

ADR 026 is the most clearly-written ADR in the repo and the one most likely to slip. It distinguishes three things that sales conversations routinely conflate — "uses a FIPS-validated module," "runs in the module's FIPS operating mode," and "carries a CMVP certificate for your binary profile." Only the third is what a regulated Fortune-500 buyer means when they ask "is Wendelon FIPS-certified?"

Today we are approximately at state (1). State (3) is the workstream output. The path between them is: (a) migrate transport TLS inside the module boundary, (b) author a Security Policy with the lab, (c) run the CMVP validation cycle, (d) ship a binary profile with a certificate number.

This workstream also covers supply-chain compliance (SBOM, signed artifacts, SLSA) because the calendar is adjacent: a customer asking for CMVP will also ask for these, and shipping them alongside the certification posture is cheaper than shipping them later.

## 2. Strategic position (opinionated)

### 2.1 Transport TLS migrates inside the module boundary; it is non-negotiable for the regulated SKU

**Position:** `rustls-aws-lc-rs` replaces `rustls`+`ring` in the FIPS build profile. The FIPS module boundary expands to include transport. The default (non-FIPS) build can keep `ring` if it's cheaper; it's irrelevant to the regulated SKU.

**Why.**

Fortune-500 regulated buyers — FedRAMP High, DoD SRG IL5+, some state PRAMPs, banking regulators reading FFIEC — will not accept a FIPS posture that excludes the transport. The argument "transport is outside the module boundary" is a valid design choice but is not a procurement-acceptable posture. The fix exists: `rustls-aws-lc-rs` is a real provider backed by the same AWS team that maintains `aws-lc-rs`.

**Trade-off.** `rustls-aws-lc-rs` upstream is less mature than `ring` and introduces an upstream dependency we partly depend on. Mitigation is in §2.2.

**What if `rustls-aws-lc-rs` is not stable enough at pilot time?** We contribute. A customer-facing pilot cannot ship the FIPS SKU until transport is inside the boundary; there is no way to market that SKU honestly without it. The engineering cost of contributing to the upstream provider is less than the revenue cost of delaying the regulated SKU by two quarters waiting for stability.

**Explicit fallback posture (from Codex's open question on `rustls-aws-lc-rs` maturity).** If the upstream provider is not production-ready by Phase 1 Month 6, the regulated SKU does not ship as "FIPS 140-3 certified" at Phase 2 exit. Instead, the SKU ships as one of:

- **Posture A (preferred if provider is close-but-not-ready):** "FIPS-ready, transport-inside-module-boundary in progress, CMVP certification submitted." The FIPS build uses a pinned pre-GA release of `rustls-aws-lc-rs`; the Security Policy explicitly notes transport is in-scope; the CMVP submission proceeds; the pilot customer is told certification will land in the normal validation cycle.
- **Posture B (fallback if provider is materially blocked):** "FIPS-ready at-rest module; transport outside module boundary; full-boundary certification targeted Phase 3." The FIPS build uses `ring` for TLS and `aws-lc-rs` for at-rest crypto; ADR 026 §2 language about transport-outside-boundary remains as-written; the CMVP submission covers only the at-rest module; a second submission is scheduled once transport lands.

**Posture A is the default target; Posture B is the documented escape hatch.** The decision point is Phase 1 Month 6 (mid-Phase-1). A Design-Lock entry (DL-Exxx) records which posture shipped.

### 2.2 CMVP lab contract starts in Phase 0 regardless of code readiness

**Position:** sign the lab contract in Phase 0 Week 4. Kickoff in Week 6. Security Policy authoring begins immediately.

**Why.**

CMVP lab calendar is the single longest dependency in the entire program. Queues are 3-6 months; validation cycles are another 3-6 months. Total 6-12 months from engagement to certificate. If engineering waits until code is complete to engage the lab, we add 6-12 months to regulated-GA.

The Security Policy is a document; it can be drafted in parallel with engineering. Lab-asked questions are answered as they come; many Security Policy fields ("platform coverage," "entropy source statement") are independent of final code shape.

**Contract shape (architect position).** Fixed-scope, fixed-milestone. Labs that quote T&M on a Level-1 software module are either inexperienced or billing-padding. The work is bounded: one Security Policy, one set of platform assertions, one set of KATs and conditional tests. Milestones: Security Policy draft (Month 2), Security Policy final (Month 4), KAT evidence (Month 6), submission package (Month 8), validation completion (Month 10-14). Pay per milestone.

### 2.3 Symbol sweep is a regression guard, not certification evidence

**Position:** keep `ci/check_fips_symbol_sweep.sh` for what it is — a cheap regression check that AES-GCM-SIV symbols do not leak into FIPS builds. Do not describe it to customers as FIPS validation. CMVP evidence is the Security Policy plus KATs plus the CMVP certificate number.

This is a positioning fix, not an engineering change: the current `ops/key_rotation.md` leans on the symbol sweep as evidence, which is fine internally but misleading externally.

**Codex and Claude converge on this verbatim.** Codex's WS-J puts it the same way: *"do not let the symbol sweep be described as full FIPS validation."* This is the single item where the peer review confirmed an already-held position without refinement.

### 2.3a Pilot-language acceptance criteria (adopted from Codex's open question)

Customer-facing language during the pre-certificate window must be one of the following, exact strings:

- **Before CMVP submission:** "FIPS-ready." No claim of certification, no claim of in-progress.
- **After CMVP submission, pre-validation:** "FIPS-ready; CMVP certification submitted." Acceptable during Phase 1 exit → Phase 2 validation cycle.
- **After validation-in-progress:** "FIPS-ready; CMVP certification in validation cycle at [lab name]." Adds lab attribution once publicly disclosed.
- **Post-certificate:** "FIPS 140-3 validated, CMVP certificate #NNNN."

No other customer-facing FIPS language is acceptable. The compliance lead is the sole author of FIPS-claim text; engineering cannot unilaterally describe the product as "FIPS-compatible" or "FIPS-compliant" in documentation.

### 2.4 Supply-chain: SBOM at Phase 1, signed artifacts at Phase 1, SLSA L3 at Phase 2-3

**Position:**

- SBOM (SPDX 2.3 or 3.0) generated per release in Phase 1. `cargo sbom` or `syft` against the built binary.
- Signed release artifacts (cosign / sigstore keyless) shipped at Phase 1 GA.
- SLSA L2 posture documented at Phase 1 exit: hosted build environment, authenticated provenance, retained build logs.
- SLSA L3 target at Phase 2 exit: hardened build environment, isolated builds, non-falsifiable provenance.

**Why not SLSA L4.** L4 requires two-party review of every change to the build process and hermetic sealed builds. The engineering cost is high; the regulatory requirement is rare. L3 is what FedRAMP High and DoD SRG IL5+ ask for.

**Why SLSA at all.** "Supply-chain attacks" have shifted from theoretical to mainstream (SolarWinds, xz-utils). Regulated customers require SBOM and signing. SLSA is the common vocabulary for the posture.

### 2.5 Customer compliance pack is a GA-time artifact, authored Phase 2

**Position:** author a compliance pack at Phase 2 that covers SOC 2 Type 2 scoping, PCI-DSS applicability statement, HIPAA covered-entity mapping, and NIST SP 800-53 Rev 5 control mapping.

**Why.** Procurement conversations in regulated industries ask for these documents before they ask for an architecture overview. Shipping the compliance pack alongside GA compresses the sales cycle measurably.

**Architect-reviewed, compliance-authored.** The compliance lead authors; the architect reviews technical accuracy. This is the right division of labor.

## 3. Design — the components

### 3.1 `rustls-aws-lc-rs` transport migration

**Scope.**

- Replace `rustls::crypto::ring` with `rustls::crypto::aws_lc_rs` in the FIPS build profile.
- Audit the TLS-cipher-suite list: FIPS build allows only FIPS-approved suites (AES-GCM with ECDHE key exchange; no ChaCha20-Poly1305).
- Certificate handshake flow unchanged; provider change is a compile-time feature.
- Integration test: FIPS build binary presents only FIPS-approved cipher suites in the `ClientHello`.

**Risk surface.**

- `rustls-aws-lc-rs` upstream bugs become Wendelon bugs.
- `rustls` API churn at major-version bumps affects both providers but the FIPS provider historically lags.
- Performance regression on high-connection-count workloads (benchmark before/after).

**Mitigation.**

- 0.5 FTE budget for upstream contribution in Phase 1. If the upstream provider is missing features we need (e.g., specific cipher suite), we contribute the PR rather than work around.
- Pin to a specific `rustls-aws-lc-rs` version in the FIPS profile; updates are deliberate and tested.

### 3.2 Security Policy authoring

**Scope (what the document contains).**

1. Module identification: name, version, build profile, platform list.
2. Module boundary: which code paths are inside (at-rest crypto per ADR 019/020 + transport per §3.1 + audit chain hash under FIPS per ADR 025 amendment + identity signatures per WS-C), which are outside.
3. Cryptographic primitives: AES-256-GCM (plain-GCM branch with injective nonces), AES-KW, HKDF-SHA-256, ECDSA-P256 (if that's the signature choice), SHA-256.
4. Approved-mode enforcement: how the module prevents non-approved primitives at runtime.
5. Self-tests: power-up KATs, conditional tests (e.g., pairwise consistency on key gen).
6. Entropy source: `/dev/urandom` backed by `getrandom(2)`, with platform assertion.
7. Crypto Officer and User roles (map to Wendelon's `cluster_admin` and `data_node` respectively; `compliance_auditor` maps to User with read-only).
8. Physical security: N/A for Level 1.
9. Operational environment: hosted Linux x86_64 and aarch64, kernel version floor.

**Authoring flow.**
- Architect + compliance lead + lab author.
- Lab provides template and asks questions; architect answers architecturally; compliance lead ensures CMVP-form compliance.
- Architect review gate before lab submission: every assertion traceable to code.

### 3.3 KATs and conditional tests

**Scope.** Known-answer tests for every primitive used in the FIPS build:

- AES-256-GCM: NIST CAVS test vectors, pass at startup.
- HKDF-SHA-256: NIST SP 800-108 test vectors.
- ECDSA-P256 (signature primitive): FIPS 186-4 test vectors.
- SHA-256 (audit chain under FIPS): NIST FIPS 180-4 test vectors.

**Conditional tests.** Per SP 800-90A:
- Continuous RNG test (if any RNG other than kernel-sourced).
- Pairwise consistency test on key generation (ECDSA keypair).

**Implementation.**
- KATs ship as `src/crypto/fips/kat.rs`.
- Bootstrap self-test: every FIPS build runs KATs at startup, aborts if any fails.
- Metric: `fips_module_self_test_total{outcome="ok"}` per ADR 027 §7.

### 3.4 SBOM generation

**Tool.** `cargo-sbom` for SPDX 2.3 output. Also `syft` against the built binary as independent verification (two-tool discrepancy check).

**Fields.**
- Every crate dependency with version.
- Every system dependency (glibc version, openssl/bssl).
- License declarations.
- Supplier (upstream maintainer) for each dependency.

**Automation.** Generated on every release; checked in as `releases/v<version>/sbom.spdx.json`. Published to the release page.

**Signing.** SBOM itself is signed; customers verify the SBOM matches the binary before deployment.

### 3.5 Artifact signing (cosign / sigstore)

**Tool.** `cosign` with keyless signing backed by the public sigstore infrastructure. Private sigstore deployment optional for air-gapped customers.

**Signed artifacts.**
- Release binaries (one per platform: `wendelon-linux-x86_64-fips`, `wendelon-linux-aarch64-fips`, `wendelon-linux-x86_64`, `wendelon-linux-aarch64`, plus `wendelon-cert-service-*`, `wendelon-operator-*`).
- SBOM.
- Container images (if we ship them).

**Verification flow documented.** `wendelon-ctl verify --artifact <path> --expected-signature <sig>` provides customer-side verification.

### 3.6 SLSA L3 posture

**Requirements for L3.**
- Source: version-controlled, access-controlled (GitHub with branch protection + required reviews: already in place).
- Build: hosted and authenticated (GitHub Actions with OIDC), isolated (separate runners per build; no cross-build state leak), parameterless (no manually-overridden parameters), hermetic (all inputs declared).
- Provenance: non-falsifiable, available to consumers.

**Gap from current state.**
- Build hermeticity: Cargo's `registry` dependencies fetch from crates.io at build time; for hermeticity, we vendor dependencies and build offline. Already partly done (`cargo vendor`); need to complete.
- Isolated builds: GitHub Actions default runners are shared; need `self-hosted` runners with per-build isolation or pay for GitHub Enterprise's isolated runners.
- Provenance generation: SLSA GitHub Generator action produces in-toto provenance. Integrate.

**Target: Phase 2 exit.**

### 3.7 Customer compliance pack

**Deliverables (authored by compliance lead, reviewed by architect):**

1. **SOC 2 Type 2 readiness document.** Trust Services Criteria (Security, Availability, Processing Integrity, Confidentiality, Privacy) mapped to Wendelon controls. Gap analysis against customer's SOC 2 scope.
2. **PCI-DSS 4.0 applicability statement.** Section-by-section (1.x through 12.x) assessment of which controls Wendelon inherits from the customer and which it satisfies directly.
3. **HIPAA Security Rule mapping.** 45 CFR §164.308 (administrative), §164.310 (physical), §164.312 (technical) safeguards mapped to Wendelon architecture.
4. **NIST SP 800-53 Rev 5 control mapping.** AC, AU, CM, CP, IA, SC, SI families mapped to Wendelon architectural features. Inheritance matrix: what the customer's hosting provider provides vs. what Wendelon provides vs. what the customer provides.
5. **FedRAMP Moderate/High gap analysis** (if applicable to customer).
6. **DoD SRG IL4/IL5 applicability** (if applicable to customer).

**Format.** PDF for customer distribution, Markdown source in `plans/compliance/` for version control.

**Maintenance.** Refreshed per release. Out-of-date compliance packs are a procurement red flag.

## 4. Work plan (Phase-aligned)

### Phase 0 (Weeks 1-6)

| Week | Deliverable |
|---|---|
| 1 | Lab RFP issued. |
| 3 | Lab selected; contract in negotiation. |
| 4 | ADR 026 accepted (Open Questions closed). Lab contract signed. |
| 5 | Lab kickoff meeting. Platform list shared. Security Policy outline drafted. |
| 5 | SBOM tooling prototyped (`cargo-sbom` evaluation). |
| 6 | Signed-artifact infrastructure prototyped (cosign). |

### Phase 1 (Months 2-10)

| Month | Deliverable |
|---|---|
| 2 | `rustls-aws-lc-rs` migration begins. FIPS build profile initial CI green. |
| 3 | Security Policy draft 1 submitted to lab. |
| 3 | FIPS build profile: TLS handshake integration-tested; cipher suite restriction enforced. |
| 4 | KATs land; bootstrap self-test aborts on failure. |
| 4 | SBOM generated per release. Signed release artifacts shipping. |
| 5 | Security Policy draft 2 (lab feedback incorporated). |
| 5 | SLSA L2 posture documented; provenance generation wired. |
| 6 | `rustls-aws-lc-rs` migration complete. Performance benchmark vs. `ring` acceptable. |
| 7 | Security Policy final. |
| 8 | KAT evidence package submitted. Conditional test evidence package submitted. |
| 9 | CMVP submission package assembled. |
| 10 | Phase 1 exit: submission in the CMVP queue. FIPS build profile stable. SBOM + signed artifacts on every release. |

### Phase 2 (Months 10-15)

| Month | Deliverable |
|---|---|
| 10-14 | CMVP validation cycle. Lab-driven; architect responds to lab questions. |
| 11 | SLSA L3 posture achieved (hermetic builds, isolated runners, non-falsifiable provenance). |
| 12 | Customer compliance pack authored. |
| 13 | Customer compliance pack reviewed with first regulated pilot customer. |
| 14 | CMVP certificate issued (target). |
| 14 | ADR 026 updated with CMVP certificate number. |
| 15 | Phase 2 exit: FIPS SKU ships with CMVP number; compliance pack in hand. |

## 5. Interfaces with other workstreams

- **WS-A (Governance):** ADR 026 acceptance (Phase 0), CMVP lab contract (Phase 0).
- **WS-B (Crypto):** all crypto primitives route through `aws-lc-rs` in the FIPS build. WS-B providers must honor FIPS build's compile-time exclusion of GCM-SIV.
- **WS-C (Identity):** Ed25519 is not FIPS-approved; signature primitive becomes ECDSA-P256 in the FIPS build. WS-C's proposal-signature format is build-profile-dependent.
- **WS-D (Audit):** hash primitive switches blake3 → SHA-256 under FIPS. `hash_algorithm_tag` in events already accommodates this.
- **WS2 (Codex's Observability):** FIPS-build metrics (`fips_module_self_test_total`, `fips_module_version_info`) need emission wired.
- **WS1 (Codex's Hot-path):** `rustls-aws-lc-rs` migration may affect QUIC handshake benchmarks.

## 6. Exit criteria

1. ADR 026 accepted with all Open Questions closed.
2. `rustls-aws-lc-rs` shipping in the FIPS build profile; transport inside the module boundary.
3. Security Policy submitted to CMVP lab (Phase 1 exit).
4. CMVP certificate issued and recorded in ADR 026 (Phase 2 exit).
5. SBOM generated per release; shipped with release artifacts.
6. Release artifacts signed via cosign/sigstore; customer verification flow documented.
7. SLSA L3 posture achieved for the FIPS build profile.
8. Customer compliance pack published: SOC 2, PCI-DSS, HIPAA, NIST 800-53 mappings.
9. FIPS-build metrics (self-test, module-version) emitting; observability integration green.
10. `ci/check_fips_symbol_sweep.sh` retained as regression guard; described accurately in `ops/key_rotation.md`.
11. Scorecard row "Compliance / FIPS evidence" green at each gate target.

## 7. Risks within this workstream

- **CMVP lab queue extends past 12 months.** Mitigation: start engagement Phase 0; do not gate on code readiness. If queue extends beyond Phase 2 exit, ship "FIPS-ready, certification in progress" per ADR 026's two-state roadmap.
- **`rustls-aws-lc-rs` upstream instability.** Mitigation: 0.5 FTE contribution budget; pin to specific versions; integration test against known-good combinations.
- **Security Policy author disagreement with lab.** Mitigation: architect has technical-accuracy veto; compliance lead handles CMVP-form compliance; escalate to architect lead for ambiguous calls.
- **Customer compliance pack out of scope grows.** Customers ask for ISO 27001, SOC 1, specific industry frameworks (e.g., NERC CIP for utilities). Mitigation: pack v1 covers the big four (SOC 2, PCI-DSS, HIPAA, NIST 800-53); additional frameworks are consulting-engagement artifacts.
- **SLSA L3 isolation costs.** Self-hosted runners with per-build isolation are operationally non-trivial. Mitigation: budget a small SRE contribution; alternative is GitHub Enterprise isolated runners at a cost.
- **ECDSA-P256 performance vs. Ed25519 in the signature hot path.** ECDSA is slower. Mitigation: signature verification is not a measurable hot path in Wendelon (not on every request); the cost is absorbed.
- **Upstream `aws-lc-rs` CMVP status changes mid-validation.** If the pinned aws-lc-rs version's CMVP status changes (certificate withdrawn, etc.), the Wendelon validation may be blocked. Mitigation: pin to a specific already-CMVP-validated version; upgrade only on the next validation cycle.

## 8. Points of agreement and divergence with Codex's WS-J

**Agreement (near-verbatim on multiple points):**
- CMVP lab work starts immediately because the calendar is long.
- Transport-boundary truth matters as much as primitive selection.
- Symbol sweep is a regression test, not a marketing claim.
- Supply-chain artifacts (SBOM, signing, SLSA) are mandatory for regulated release classes.
- FIPS-ready and FIPS-certified are distinct; language must reflect the difference.

**Refinement from Codex:**
- Explicit pilot-language acceptance criteria now in §2.3a (what can and cannot be said to customers pre-certificate).
- Explicit fallback posture (Posture A / Posture B) now in §2.1 for the `rustls-aws-lc-rs` upstream risk.

**Consistent independently:** both arrived at `rustls-aws-lc-rs` as the regulated-SKU transport path and ECDSA-P256 as the FIPS-approved signature primitive.

**Not contested:** everything else.

## 9. Opinionated Constraints (framing adopted from Codex)

- Do not let the symbol sweep be described as full FIPS validation.
- Do not claim a certified transport posture until the transport path is actually inside the defensible boundary.
- Do not leave SBOM/signing/SLSA until release week.
- (Added by Claude:) Do not let engineering write FIPS claims in customer-facing docs; the compliance lead is the sole author.
- (Added by Claude:) Do not land crypto-boundary changes after CMVP certificate issuance without a formal boundary-changing-PR review; re-validation timing is a customer commitment.

## 10. Open questions

1. **Signature primitive choice (Ed25519 vs ECDSA-P256).** This spans WS-C and WS-E. I argue ECDSA-P256 for both build profiles (uniform format, FIPS-approved). Counter-argument: Ed25519 is faster and sufficient for non-FIPS builds. Where does the council fall?

2. **CMVP lab selection.** Procurement-driven, but architect cares about Rust familiarity. Candidates: atsec, Leidos, Acumen, UL. Does the council have a preference based on past CMVP validations in Rust-language modules?

3. **Transport boundary in the FIPS build: full `rustls-aws-lc-rs` or hybrid.** A hybrid posture ships FIPS-approved cipher suites via `aws-lc-rs` but leaves handshake state machine in `rustls`-default code. Simpler; may not satisfy lab. Full migration is more work but cleaner boundary. I lean full; want lab's initial reaction.

4. **Platform coverage for CMVP.** Level 1 allows a broad platform set, but each platform adds validation cost. I propose Linux x86_64 and aarch64 only for initial CMVP. Windows/macOS/FreeBSD deferred. Is this enough for pilot customers?

5. **Kernel-version floor for entropy statement.** `getrandom(2)` has varied guarantees across kernel versions. I propose floor = Linux 5.15 (LTS, supported through 2027). Customers on RHEL 8 (kernel 4.18) would be out of scope for FIPS SKU; they'd run the default SKU. Acceptable?

6. **SBOM tool choice.** `cargo-sbom`, `syft`, `cyclonedx-rust-cargo`. Each has quirks in SPDX 2.3 conformance. Use two and reconcile, or pick one? I lean use both as a discrepancy check; pick primary based on SPDX 2.3 validator output.

7. **Customer compliance pack: generic or customer-specific?** I proposed generic mappings. But a customer with a specific audit scope may want a pack tailored to their environment. Do we productize a generic pack and offer customer-specific consulting for tailoring?

8. **SLSA provenance consumer tooling.** A customer needs to verify SLSA provenance. We ship the provenance; do we ship the verification tool or rely on the SLSA reference verifier (`slsa-verifier`)? I lean rely on the reference verifier and document its use; customer-side verification is their responsibility.

9. **FIPS build CI cost.** FIPS builds with FIPS-profile tests double the CI matrix. Current CI runs a symbol sweep on FIPS build; that's cheap. KAT execution at every CI run is cheap too. Full cipher-suite negotiation test is cheap. Is there a less cheap test I'm missing? Mitigation: run full FIPS-profile test suite nightly, not per-PR.

10. **Post-CMVP-certificate maintenance.** Once the certificate is issued, any change to the crypto module boundary requires re-validation. What is the process for landing a crypto-boundary PR post-certificate? I propose: a formal "boundary-changing PR" label, architect sign-off required, re-submission to lab for evaluated scope, re-certification timing communicated to customers. Needs Design-Lock entry.

The first three questions gate Phase 1 scope; the rest can be Design-Lock entries through Phase 0 and Phase 1.
