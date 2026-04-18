# ADR 026: FIPS 140-3 Certification Target and Module Boundary

## Status
Proposed

## Context
Regulated-industry deployments require FIPS 140-3 validation for the cryptographic primitives a product uses. "FIPS compatibility" has three distinct meanings that often get conflated:

1. **Using a FIPS-validated module** — all primitives resolve through a module that has (or is pursuing) CMVP validation.
2. **Running in the module's FIPS operating mode** — the module's FIPS-only constraints are enforced (no MD5, no non-approved DRBGs, etc.).
3. **Carrying a CMVP certificate for your specific product/binary profile** — the full certification endpoint, which is what customers in regulated industries actually require on a purchase order.

Earlier drafts of Wendelon's FIPS position conflated (1) with (3). A module-only posture is cheap to ship and is correctly described as "FIPS-ready," but it is not what a customer means when they ask "is Wendelon FIPS-certified?" Closing the distinction is load-bearing.

Forces in play:
- ADR 019 selected **aws-lc-rs** as the at-rest crypto provider precisely because it has an active CMVP submission path. Transport TLS remains on `rustls`/`ring` unchanged (ring has no FIPS story, but transport TLS is outside the symmetric-at-rest module boundary in ADR 019's partitioning).
- The customer-facing promise must be testable: "FIPS-certified" is a binary property, not a gradient. A roadmap that ends at module linkage and calls itself done will be rejected by procurement.
- CMVP lab engagement has calendar cost that is independent of engineering cost. Starting the lab engagement late pushes certification well past code-complete.
- Heterogeneous deployments exist: some customers want the FIPS binary, others do not want the added dependency weight. The architecture must support both from one codebase without permanent feature-flag maintenance.
- The cryptographic module boundary — what is "inside" the FIPS module vs. "outside" — determines what the CMVP certificate covers. Getting this boundary wrong means either over-scoping (certifying more code than necessary) or under-scoping (certifying too little and failing a customer audit).

## Decision

1. **Full FIPS 140-3 certification is the finish line, not module linkage.** The perfection endpoint is a CMVP certificate for a named binary/deployment profile — the "Wendelon FIPS Build" — not a claim of "we use aws-lc-rs." Module-only linkage is an interim milestone on the way to full certification, explicitly not the end state.

   Two states are supported on the roadmap:
   - **FIPS-ready (interim).** Binary builds that link only aws-lc-rs for cryptographic primitives, run aws-lc-rs in its FIPS operating mode, and exclude non-validated primitives by compile-time gating. Customers who require "uses a FIPS-validated module" can deploy this today. Wendelon does not claim a CMVP certificate in this state.
   - **FIPS-certified (endpoint).** The above plus a CMVP certificate for the specific Wendelon FIPS Build binary profile. Customers who require CMVP-certified software deploy this build.

   The procurement question — "is Wendelon FIPS 140-3 certified?" — has a binary answer per state: "FIPS-ready, certification in progress, lab engagement underway at [lab]" or "FIPS 140-3 validated, CMVP certificate #NNNN."

2. **aws-lc-rs is the cryptographic module backend.** All at-rest symmetric encryption (L2 envelope, WAL, redb values per ADR 020), key-derivation functions, signature primitives, and audit hash primitives (ADR 025) route through aws-lc-rs. Transport TLS continues to use `rustls`/`ring` per ADR 019; the TLS stack lives outside the FIPS module boundary in the current partitioning, on the explicit condition that the FIPS build profile can still terminate mTLS with a validated stack (see Open Questions — transport-side FIPS is tracked separately).

   The FIPS build profile enforces this via:
   - Compile-time feature flags that exclude alternate backends.
   - A compile-time assertion that `cipher_id` (ADR 020 §2) resolves only to **AES-256-GCM with the injective structural-nonce encoding** (ADR 019 plain-GCM branch, `cipher_id = 1`). The ADR 019 GCM-SIV branch (`cipher_id = 0`) is compiled out of the FIPS binary; see decision §2a.
   - A run-time self-test hook (`FIPS_mode_set()` equivalent on aws-lc-rs) verified in the FIPS binary's startup path.

2a. **AES-256-GCM-SIV is not FIPS-approved and is excluded from the FIPS build.** AES-256-GCM-SIV (RFC 8452) is an IETF-standardized misuse-resistant construction. It is not on the NIST approved-mode list (SP 800-38 family; CAVP tests cover GCM/GMAC/XPN but not GCM-SIV). The FIPS build profile therefore forces ADR 019's plain-GCM fallback branch as the only active cipher path — not as a fallback, but as the unconditional selection under the FIPS profile. This is not a regression in safety: ADR 019's plain-GCM branch is defined with the injective structural-nonce encoding (`version(u32be) || chunk_index(u64be)`) and the per-DEK data bound (`slab_max_bytes ≤ 2^35` bytes), both of which are retained unchanged. The FIPS build's cipher path is the plain-GCM branch with injective nonces — cryptographically safe by construction, NIST-approved by primitive, and already fully specified by ADR 019.

   This ADR **amends ADR 019**: the decision tree in ADR 019 §Decision is for the default build profile. Under the FIPS build profile, the tree collapses to one branch — AES-256-GCM with the injective nonce encoding and the per-DEK data bound, regardless of aws-lc-rs's GCM-SIV availability. If upstream NIST guidance later adds GCM-SIV to the approved-mode list, this ADR re-opens to consider re-enabling the GCM-SIV branch in the FIPS profile.

3. **Cryptographic module boundary is documented and stable.** The boundary is the set of code paths that invoke a primitive on plaintext key material or protected data. Within the boundary:
   - At-rest symmetric encryption (ADR 019/020): **default build** — AES-256-GCM-SIV when backend coverage is present, AES-256-GCM with injective nonces otherwise; **FIPS build** — AES-256-GCM with injective nonces only (SIV compiled out, per §2a).
   - Key wrapping (ADR 020): AES-KW or the AES-GCM-based wrapping chosen by aws-lc-rs.
   - Key derivation: HKDF-SHA-256 (from aws-lc-rs).
   - Signature: the signature primitive used for audit signed roots (ADR 025 §4) and any future signed artifact — presumptively ECDSA P-256 from aws-lc-rs, subject to acceptance-time verification.
   - Hash: blake3 for content addressing is **outside** the module boundary (not a FIPS-approved primitive). SHA-256 is the approved hash and is used wherever a chain or signature interacts with FIPS-gated code.

   **Consequence of the blake3 boundary.** ADR 025's audit chain uses blake3. For the FIPS build profile, ADR 025's chain hash primitive switches to SHA-256, produced by aws-lc-rs. The canonical-CBOR serialization is unchanged. The chain-head RPC's output includes a `hash_algorithm` tag so consumers can distinguish. This ADR **amends ADR 025**: its chain primitive is parameterized, blake3 for non-FIPS builds and SHA-256 for FIPS builds.

4. **No non-validated primitives in the FIPS binary.** Dead crypto paths are not merely unused — they are absent. Specifically:
   - No `ring`-provided symmetric primitives in the FIPS binary (transport TLS excepted per §2).
   - No non-aws-lc-rs hash, MAC, or signature backends linked.
   - No debug/test crypto paths (mock cipher, null MAC) linked.
   - Compile-time feature gating enforces this; a FIPS-build CI job attempts to build with disallowed backends and fails to produce a binary.

   "No dead paths" is both a certification requirement (the CMVP lab will ask) and a correctness property (an attacker cannot downgrade to an un-validated primitive if none is linked).

5. **Documented entropy-source statement.** The FIPS build profile's DRBG seeding is documented: `/dev/urandom` on Linux backed by the kernel's getrandom(2), with a platform-assertion that the kernel's DRBG meets the SP 800-90A requirements. Where aws-lc-rs's internal DRBG is used, the internal DRBG's compliance is cited from the module certification. The entropy-source statement is a standalone document attached to the CMVP submission.

6. **CMVP lab engagement runs as a separate workstream, started early.** Because calendar time dominates lab engagement, the lab contract starts before the FIPS binary is code-complete. The engagement produces:
   - Security Policy document (lab co-authored).
   - Platform coverage list (Wendelon's supported deployment platforms for the certified build).
   - Testable boundary definition (derived from §3 above).
   - Operational test plan (self-test, known-answer tests, conditional tests).

   Lab engagement timeline is a project-management artifact, not an architectural decision; it is tracked in `plans/ops/` rather than inline in this ADR. The ADR-level commitment is that lab engagement precedes binary feature-freeze.

7. **FIPS build profile is one build, not a run-time toggle.** The FIPS binary is produced by a distinct build configuration (`cargo build --profile fips --features fips,...`). Toggling at run-time is explicitly rejected: a run-time toggle requires linking both validated and non-validated primitives, which fails §4's "no dead paths" rule and forces the CMVP boundary to cover more code than necessary.

   Non-FIPS deployments run the default binary. FIPS deployments run the FIPS binary. The feature set is the same; the crypto backend is not.

## Rationale

- **"FIPS-certified" is a procurement-visible binary property.** Conflating module-linkage with certification misrepresents the product to customers. The two-state roadmap (ready → certified) is honest and matches what procurement actually asks for.
- **Module-only as interim is not a compromise, it is a milestone.** Customers who can accept "FIPS-ready" deploy it immediately; customers who need the certificate wait for the lab engagement. Both paths are real; the architecture serves both.
- **Single-backend via aws-lc-rs minimizes the validation surface.** A multi-backend FIPS story requires certifying each backend's interaction; single-backend certifies one thing. aws-lc-rs is the only Rust-native backend with an active CMVP submission path, which pins the choice.
- **Transport-TLS outside the boundary is correct for the pilot.** `rustls`/`ring` has no FIPS story; requiring `rustls-aws-lc-rs` (which exists but has deployment friction) is the eventual state. The partitioning — at-rest module is validated now; transport module is validated later — matches the actual certification timeline.
- **Blake3 outside the boundary is an informed trade-off.** Blake3 is correct and fast; it is not FIPS-approved. The audit chain is the only consumer in the FIPS build profile, and SHA-256 is fast enough for audit volumes. Switching is a one-line change behind a build flag.
- **No run-time toggle eliminates a foot-gun.** A "FIPS mode: enabled/disabled" run-time flag is the classic compliance theater: the binary still contains the non-validated code paths, and an operator who toggles off thinks they are "faster" while losing certification. One build, one posture.
- **Lab engagement calendar dominates engineering calendar.** CMVP lab queues are measured in months; starting the lab engagement after code-complete adds that many months to certification. Starting early costs lab fees during a window when iteration is still possible, but it does not push the certification date.

## Consequences (The "Negatives")

- **Two build profiles to maintain forever.** `default` and `fips`. CI must build, test, and package both on every release. Mitigated by the feature-flag infrastructure already in place; the cost is real but bounded to the crypto-boundary modules.
- **Blake3→SHA-256 switch in the audit chain means the chain-head RPC output is build-profile-dependent.** Auditors consuming chain heads from a FIPS cluster see SHA-256 digests; from a non-FIPS cluster see blake3 digests. Mitigated by the `hash_algorithm` tag in the chain head record; downstream tooling must handle both.
- **Cross-build-profile cluster operation is unsupported.** A mixed cluster (some nodes FIPS, some nodes non-FIPS) is not a supported topology. The crypto backends have different primitives enabled and the audit chains' hashes are incompatible. The supported operation is: upgrade all nodes to the same build profile. Mitigated by documented bring-up and upgrade procedures in `plans/ops/upgrades.md`.
- **Platform coverage is narrower for the FIPS profile.** CMVP certification is per-platform. The FIPS build profile certifies a specific platform list (e.g., Linux x86_64 and aarch64 on specific glibc versions); the default profile runs everywhere Rust does. Mitigated by documenting the FIPS platform list as part of the product SKU.
- **Lab engagement is an ongoing cost.** Re-certification on major code changes, annual cost, and platform additions are real. Accepted because the revenue from regulated-industry deployments requires it.
- **Rejected alternatives explicitly:**
  - *Module-only as the finish line* — rejected; conflates linkage with certification and misrepresents the product.
  - *Level 4 hardware module* — rejected; out of scope for a software distributed system and enormously over-engineered for the threat model.
  - *Self-certification* — rejected; CMVP is the accepted certification path and there is no substitute.
  - *Run-time FIPS toggle* — rejected; violates the "no dead paths" rule and surrenders certification to an operator setting.
  - *Multi-backend FIPS (aws-lc-rs + something else)* — rejected; multiplies the validation surface and buys nothing.
  - *Deferring FIPS to post-pilot* — rejected; the pilot's customer base includes regulated-industry deployments, and retrofit would require re-architecting the crypto boundary.

## Open Questions

All items must be closed before this ADR moves from Proposed to Accepted.

- **aws-lc-rs FIPS certification state at chosen version.** Whether the pinned aws-lc-rs version has an active CMVP certificate (or submission in progress). If not, the FIPS build profile is gated on the upstream status. *Resolves via:* verification against the upstream CMVP status page at the pinned version, recorded as a single line in this ADR at acceptance time. Blocking if no certificate exists.
- **Transport TLS FIPS story.** `rustls`/`ring` is not FIPS-validated. Eventual migration path — `rustls-aws-lc-rs` provider — is on the table but not in this ADR. *Resolves via:* a follow-up ADR (or amendment to ADR 011/004) defining the FIPS transport backend and its acceptance criteria. The FIPS build profile for the pilot ships with the current `rustls`/`ring` transport and a documented statement that transport TLS is outside the current module boundary.
- **Entropy-source statement platform coverage.** The getrandom(2)-backed DRBG assertion holds on modern Linux; specific kernel-version floors for the certified platforms require verification. *Resolves via:* the Security Policy document produced during lab engagement.
- **Lab selection.** The specific CMVP-accredited lab for the engagement (atsec, Leidos, Acumen, etc.) is a procurement choice, not an architectural one. *Resolves via:* a separate procurement decision; ADR acceptance does not require a specific lab.
- ~~**Audit-chain primitive switch (ADR 025 amendment).**~~ **Resolved.** ADR 025 decision §1 and §3 already parameterize the chain hash as `hash_func` (blake3 by default, SHA-256 under the FIPS build profile) and expose the `hash_algorithm_tag` in the chain head. No further ADR 025 edit is required; the cross-reference is already in ADR 025 §3's text and in its References list.

## References
- ADR 004: Zero-copy Transport (transport TLS; outside current module boundary)
- ADR 011: Auto Adaptive Transport (transport TLS; same)
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization (identity stack)
- ADR 019: Cipher Selection and FIPS Posture (primitive selection; aws-lc-rs backend)
- ADR 020: L2 Envelope Encryption and DEK Lifecycle (key hierarchy under validated primitives)
- ADR 025: Audit Event Model (amended: chain primitive is build-profile-dependent)
- `plans/ops/upgrades.md` — documented FIPS upgrade procedure
