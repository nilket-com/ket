# ADR 019: Cipher Selection and FIPS Posture

## Status
Proposed

## Context
Wendelon needs a single, clear answer to "which symmetric authenticated-encryption cipher do we use?" that holds across every context where symmetric encryption applies: L2 envelope-encrypted object payloads (ADR 020, planned), WAL segment encryption, redb values (ADR 017), and any future at-rest or in-motion crypto.

Forces in play:
- **AES-256-GCM is the obvious default**, but its 96-bit nonce makes misuse catastrophic: a single nonce reuse under the same key leaks the authentication key and recovers plaintext. At billions-of-records scale, the probability analysis for structural-nonce schemes is not trivial, and the engineering discipline required to guarantee uniqueness across partitions, crashes, and concurrent writers is fragile. This was Gemini's central red-flag during review rounds.
- **AES-256-GCM-SIV** (RFC 8452) is misuse-resistant: nonce reuse degrades to a distinguisher (an adversary can tell two identical plaintext/AAD pairs produced the same ciphertext) rather than key recovery. On AES-NI hardware, GCM-SIV is 2-3× faster than XChaCha20-Poly1305.
- **XChaCha20-Poly1305** has a 192-bit nonce, giving random-nonce safety without structural-nonce gymnastics. It is not FIPS-approved in the current validated module surface.
- **FIPS 140-3 posture** is a target for regulated-industry deployments (see ADR 026, planned). The chosen primitive must be implementable via a validated cryptographic module. aws-lc-rs is the planned backend; its coverage of GCM-SIV at implementation time is the key external variable.
- Hand-rolled crypto primitives are never on the table. Only the mode-of-use and nonce derivation live in Wendelon code; the primitive itself always comes from a validated library.

## Decision

**AES-256-GCM-SIV is the preferred cipher.** When GCM-SIV is unavailable in the selected cryptographic backend, Wendelon falls back to AES-256-GCM with an injective structural nonce encoding and a per-DEK data bound.

```
Is AES-256-GCM-SIV available in the current backend?
├─ YES → use AES-256-GCM-SIV
└─ NO  → use AES-256-GCM with:
         - injective structural nonce encoding (see below)
         - per-DEK ciphertext volume bounded below the NIST safe limit
         - uniqueness + round-trip tests shipped
```

**Nonce strategy is uniform across both branches.** The 96-bit nonce is derived from the chunk ordinal within the slab:

```
nonce[0..4]  = version (u32 big-endian, currently 0)
nonce[4..12] = chunk_index within slab (u64 big-endian)
```

This encoding is **injective by construction**: distinct `chunk_index` values under one DEK produce distinct nonces. Because ADR 020 scopes DEKs per-slab, a DEK is never reused across slabs, and the chunk ordinal is sufficient — the slab identity need not appear in the nonce. No hash truncation, no collision analysis, no randomness. Uniformity across branches means one nonce strategy and one slab format regardless of which cipher the backend provides.

**Earlier drafts included a third branch (XChaCha20-Poly1305) as a non-FIPS fallback.** That branch existed to avoid structural-nonce engineering via XChaCha's 192-bit random nonce. With the injective encoding adopted above, the structural-nonce property is a construction guarantee, not a discipline problem — so the motivation for XChaCha disappears. Retiring it also removes a third code path and its non-trivial provider question (see Consequences for context). The decision tree is two branches.

**FIPS build profile override.** The decision tree above is for the default build profile. AES-256-GCM-SIV (RFC 8452) is an IETF construction and is *not* on the NIST SP 800-38 approved-mode list; CAVP covers GCM/GMAC/XPN but not GCM-SIV. Under the `fips` build profile (ADR 026 §2a), the first branch of the tree is compiled out and the plain-GCM branch with injective nonces is the only active cipher path. All acceptance-bar invariants below (per-DEK data bound, injective-nonce test, per-slab DEK invariant, reserved version byte) apply unchanged under the FIPS profile — they were designed for the fallback branch and carry over cleanly. Cipher selection is therefore a compile-time property of the build profile, not a runtime toggle. See ADR 026 §2a for the detailed justification and §3 for the no-runtime-toggle rule.

**Acceptance bar for the plain-GCM branch:**

1. **Per-DEK data bound.** NIST SP 800-38D limits AES-GCM under a single key to roughly `2^36` bytes (64 GiB) of protected data. Wendelon enforces this structurally rather than probabilistically: `slab_max_bytes` in the cluster config has a hard compile-time ceiling of `2^35` bytes (32 GiB), giving a 2× safety margin against the NIST limit. Because ADR 020 scopes DEKs per-slab, the per-DEK ciphertext volume cannot exceed `slab_max_bytes`, which in turn cannot exceed `2^35` bytes. A 64 MiB default slab is four orders of magnitude under this bound; the ADR-level invariant is the ceiling, not the default.
2. **Injective nonce encoding test.** Distinct `chunk_index` inputs produce distinct nonces across a fuzzed input space.
3. **Per-slab DEK invariant (load-bearing).** DEKs must never be reused across slabs. This is an ADR 020 guarantee; any future ADR that widens DEK scope re-opens ADR 019.
4. **Reserved version byte.** `version = 0` today. Any non-zero observed version is a decrypt error until a future ADR defines its meaning.

If any acceptance-bar invariant cannot be honored in the pinned backend, plain-GCM encryption is unavailable and the feature blocks rather than ships. There is no third "hope it works" branch.

**aws-lc-rs is the cryptographic backend for both build profiles.** At ADR acceptance time, aws-lc-rs's AES-256-GCM-SIV availability is verified against the library version pinned in the repo. The verification result is recorded in this ADR as a single line under Open Questions, and that line becomes the trigger for whether the *default* build's SIV branch is the hot path or whether the plain-GCM fallback branch is the hot path in the default build as well. In the **FIPS build profile**, the SIV branch is unconditionally compiled out (per ADR 026 §2a) — the availability check is relevant only to the default-profile decision tree.

**No hand-rolled crypto primitives.** Wendelon code may own: cipher selection, nonce derivation, AAD binding, key wrapping construction, and DEK rotation policy. Wendelon code never owns: the block cipher itself, the AEAD mode, the hash function, the KDF, or any other primitive.

**Provider assignment per cipher and build profile:**
| Cipher | FIPS build | Default (non-FIPS) build |
|--------|-----------|--------------------------|
| AES-256-GCM-SIV | **compiled out** (not FIPS-approved; see ADR 026 §2a) | `aws-lc-rs` (when coverage confirmed) |
| AES-256-GCM (injective-nonce branch) | `aws-lc-rs` — **sole active cipher path** | `aws-lc-rs` (used when SIV coverage is absent) |

The FIPS column is not "SIV when available" — SIV is structurally excluded from the FIPS build regardless of library coverage, because AES-256-GCM-SIV is not on the NIST SP 800-38 approved-mode list (IETF RFC 8452, not CAVP-covered). Cipher selection is therefore a compile-time property of the build profile, not a runtime toggle (ADR 026 §3).

The current transport stack uses `rustls` with `ring`; `ring` remains the transport TLS provider unchanged. The *symmetric encryption* for at-rest payloads (L2 slabs, WAL, redb values) uses `aws-lc-rs` exclusively, which exposes AES-256-GCM and AES-256-GCM-SIV in its high-level Rust API and is the same provider family targeted for FIPS builds. Using one provider for all at-rest crypto keeps the validation surface and test matrix single-tracked.

## Rationale

- **GCM-SIV is the correct default for a distributed system.** Nonce reuse in GCM is a known historical failure mode; it has broken production systems. Misuse-resistance is the property that maps correctly to a codebase with multiple writers, partition-driven retries, crash recovery, and concurrent DEK cache refills.
- **Injective structural-nonce plain GCM is a principled fallback, not a compromise.** An earlier draft treated structural-nonce GCM as fragile — correct for the *probabilistic* HKDF-truncate formulation that a Codex review flagged, not correct for an *injective* encoding whose uniqueness is a construction guarantee. Under ADR 020's per-slab DEK invariant, a chunk-ordinal nonce has no reuse path; safety becomes a matter of the data-volume bound, which is enforced by the `slab_max_bytes` ceiling, not a matter of collision probability.
- **Two branches beat three.** The prior three-branch design included XChaCha20-Poly1305 as a non-FIPS fallback specifically to avoid structural-nonce engineering via its 192-bit random nonce. With injective nonces now the standard, that motivation evaporates. Dropping XChaCha20 removes a third cipher path, eliminates a provider question (the pinned `ring` and `aws-lc-rs` versions do not expose XChaCha20-Poly1305 at the high-level Rust API the rest of Wendelon uses), and gives one slab format regardless of build configuration. Cipher agility remains available via the `cipher_id` footer field if we ever want to add XChaCha20 back through a future ADR.
- **aws-lc-rs is the right FIPS backend.** It is actively maintained by AWS, has a live FIPS 140-3 submission path, and integrates cleanly with Rust. Ring (the historical default) has no FIPS story. Building our own FIPS module is not on the table (see ADR 026 for the rationale on certification targets).
- **The ADR-time verification pattern is load-bearing (default build only).** Whether the default build exercises the SIV branch or the plain-GCM fallback depends on a property of an external library at a specific version (aws-lc-rs's GCM-SIV coverage at the pinned version). We pin that version and verify at acceptance, rather than writing a decision tree that depends on an unmeasured property of the world. This verification does *not* apply to the FIPS build: under the FIPS profile the decision tree collapses to plain GCM unconditionally (ADR 026 §2a), so SIV coverage is irrelevant there.

## Consequences (The "Negatives")

- **Branch-dependent testing.** Two possible primitives means two possible cipher paths through the code, each tested under its own CI matrix. Mitigated by keeping the AEAD interface uniform: the code above the primitive is cipher-agnostic; the primitive selection happens once at crate initialization.
- **Plain-GCM branch carries ADR-layering risk, not probabilistic risk.** With the injective encoding and the per-DEK data bound, nonce collision within a DEK is impossible by construction and the NIST safe-use envelope is respected. The remaining risk is ADR-layering: this branch's safety depends on ADR 020's per-slab DEK scope. If a future ADR widens DEK scope, the injectivity property is lost and the encoding must be redesigned. Mitigated by (a) the explicit acceptance-bar invariants above, (b) the compile-time `slab_max_bytes ≤ 2^35` ceiling, (c) re-evaluating at every aws-lc-rs version bump in case SIV becomes available so this branch can be retired, (d) treating any future change to DEK scope as automatically re-opening this ADR.
- **Backend lock-in.** aws-lc-rs is the blessed backend for both FIPS and non-FIPS at-rest crypto. Moving to a different backend would require revalidating the decision tree and re-running the SIV-coverage verification.
- **Performance asymmetry across branches.** GCM-SIV on AES-NI is the fastest path. Plain AES-GCM with structural nonces is roughly equivalent (same underlying primitive, trivial nonce-derivation cost). Operators running benchmarks should know which branch is active.
- **AAD binding must be correct everywhere.** Every callsite must bind the same contextual AAD (slab_id, chunk_index, tenant_id, version). A mismatch produces silently unauthenticated ciphertext on decrypt. Mitigated by a single `encrypt_slab_chunk` / `decrypt_slab_chunk` pair at the crypto-boundary layer; callers above it pass a typed context object.
- **No cipher agility at v1.** XChaCha20 is retired from the default tree; adding another AEAD later requires a new ADR and a new `cipher_id`. Acceptable: cipher agility is a property we add when we need it, not before.
- **Rejected alternatives explicitly:**
  - *AES-256-GCM with random 96-bit nonces as default* — rejected for nonce-collision probability at our record counts.
  - *XChaCha20-Poly1305 as a fallback* — rejected per the two-branch rationale above.
  - *Self-built FIPS module* — rejected (see ADR 026).
  - *Per-record unique DEKs (avoid the nonce problem by avoiding nonce reuse structurally)* — rejected for KMS fan-out cost; DEKs are per-slab with intra-slab chunks (ADR 020).
  - *Deterministic encryption (SIV without distinct nonce)* — rejected for leaking equality of plaintexts across records.

## Open Questions

All items below gate ADR 019 acceptance and must be closed before Proposed → Accepted.

- **aws-lc-rs AES-256-GCM-SIV coverage at the pinned version.** Determines which of the two branches is the hot path. *Resolves via:* read the aws-lc-rs feature matrix for the pinned version, run a smoke test calling `AES-256-GCM-SIV` through the library's high-level Rust bindings, and record the result inline here. If SIV is present, the plain-GCM branch is used only as a documented-but-unexercised fallback; if not, the plain-GCM branch is the production path and its acceptance-bar invariants must all be honored.
- **Injectivity and data-bound tests.** Two tests must ship with this ADR before acceptance: (1) fuzz distinct `chunk_index` inputs and verify the nonce encoding is unique and round-trippable; (2) assert that `slab_max_bytes` in the cluster config is ≤ `2^35` bytes at load time, and that no code path allows a single DEK to protect more ciphertext than `slab_max_bytes`. *Resolves via:* unit tests checked into the crypto crate before ADR acceptance. Acceptance additionally requires confirming ADR 020's per-slab DEK invariant is in the accepted ADR, not merely the draft.
- **AAD layout.** The AAD payload that binds slab/chunk/tenant into the authenticator must be bit-exact across code versions. *Resolved:* ADR 020 §2 (Decision point 2) defines a fixed-width 48-byte byte-concatenation layout (not CBOR). Cipher branches in this ADR consume that layout unchanged.

## References
- ADR 017: Persistent Local Index on redb (values encrypted under this ADR's cipher)
- ADR 020: L2 Envelope Encryption and DEK Lifecycle (planned — primary consumer)
- ADR 025: Audit Event Model (planned — audit signatures use this ADR's primitives)
- ADR 026: FIPS 140-3 Certification Target — §2a forces the plain-GCM branch under the FIPS build profile
