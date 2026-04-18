# ADR 020: L2 Envelope Encryption and DEK Lifecycle

## Status
Proposed

## Context
Wendelon's tier-2 storage (L2, object-store-backed slabs) must be confidential against the storage provider and against anyone with read access to the bucket. The question is not *whether* to encrypt — client-side envelope encryption is non-negotiable — but *at what granularity*, *with what key hierarchy*, *with what DEK lifecycle*, and *how the encryption layer composes with the other storage invariants* (O_DIRECT alignment from ADR 009, the cipher decision tree from ADR 019, range-GETs from the planned ADR 022).

Forces in play:
- **Range-GET support is required for L2 (ADR 022).** A point read that must decrypt a whole 64 MiB slab to return 128 bytes is operationally broken. Encryption granularity must support range-GET-sized decrypt boundaries.
- **KMS fan-out cost grows with DEK cardinality.** Per-record DEKs turn every tiny write into a KMS wrap. Per-whole-slab DEKs are KMS-friendly but break range reads. There is one sweet spot: per-slab DEKs with intra-slab chunked encryption.
- **ADR 019's FIPS-fallback nonce encoding assumes per-slab DEK scope as a load-bearing invariant.** If DEK scope widens in this ADR, the FIPS fallback branch of ADR 019 is invalidated.
- **Cloud-provider SSE-KMS is a trust-model fork.** Enabling SSE-KMS means the cloud provider sees plaintext-equivalent access via its KMS. That surrenders the confidentiality posture Wendelon is otherwise designed around.
- **Plaintext data exists in legacy deployments.** Enabling encryption on a cluster that has plaintext slabs requires a migration story — otherwise operators run in permanent mixed-mode, which is nobody's idea of a "perfection" endpoint.

Derived from `plans/gap/claude_answers_20260417.md` §5 and §4.

## Decision

1. **Client-side envelope encryption is the only trust model.** Wendelon encrypts payloads above the object-store boundary; the cloud sees only ciphertext and wrapped DEKs. SSE-KMS is not shipped, not even as an operator-toggle compliance overlay. The configuration surface has exactly one encryption mode: client-side envelope.

2. **Per-slab DEK with intra-slab 64 KiB chunked encryption.**
   - Each slab, on creation, is assigned a fresh DEK (256-bit symmetric key) via the cipher chosen by ADR 019's decision tree.
   - Within the slab, plaintext is split into fixed 64 KiB chunks. Each chunk is encrypted independently using ADR 019's injective nonce encoding — `nonce = version(u32be) || chunk_index(u64be)`. The same encoding is used for both ADR 019 branches (GCM-SIV and plain-GCM); nonces are **derived from chunk ordinal, not stored per chunk**, so chunk overhead is only the 16-byte authenticator tag.
   - **AAD bit-exact layout** — AEAD-associated data for each chunk is a fixed-width 48-byte concatenation (not CBOR, not length-prefixed; byte-exact for cryptographic reproducibility):
     ```
     offset  size  field
     ------  ----  --------------------------------------------------
       0      16   tenant_id          (UUID, network byte order)
      16      16   slab_id            (UUID, network byte order)
      32       8   chunk_index        (u64 big-endian)
      40       4   slab_format_version (u32 big-endian)
      44       1   cipher_id          (u8; 0 = GCM-SIV, 1 = GCM-injective)
      45       3   reserved           (must be 0x000000; any other value = decrypt error)
     ------  ----
     total   48
     ```
     A chunk from one slab cannot be decrypted in the context of another, even if a DEK were ever reused across slabs — which, by construction, does not happen. A fixed-width byte layout is used in preference to CBOR because AAD must be *bit-exact* across language versions and library upgrades; CBOR canonical form is bit-exact by spec but subtle to verify, and AAD has a fixed field set.
   - The per-slab DEK scope is **load-bearing**: ADR 019's plain-GCM fallback nonce encoding is correct only under this invariant. Widening DEK scope in a future ADR re-opens ADR 019.
   - `slab_max_bytes` has a hard compile-time ceiling of `2^35 bytes` (32 GiB) to stay within a 2× safety margin of the AES-GCM per-key data limit (`2^36` bytes per NIST SP 800-38D). This ceiling is load-bearing for ADR 019's plain-GCM branch and is enforced at config-load time, not merely documented.

3. **Three-tier key hierarchy: Root KEK → Tenant KEK → Slab DEK.**
   - **Root KEK** lives in the cloud KMS (AWS KMS, GCP KMS, Azure Key Vault, or on-prem HSM via PKCS#11). Wendelon never sees the Root KEK in plaintext. All Root-KEK operations (wrap, unwrap) are remote calls to the KMS.
   - **Tenant KEK** is wrapped by the Root KEK and stored (in wrapped form) in the Raft state machine. One Tenant KEK per tenant. Key rotation of a Tenant KEK is a Raft-committed operation.
   - **Slab DEK** is wrapped by the Tenant KEK and stored (in wrapped form) in the slab's metadata footer alongside the slab's encrypted payload. One Slab DEK per slab.
   - A read requires: unwrap Slab DEK using Tenant KEK (cheap, local), then decrypt chunks. A cold read requires: unwrap Tenant KEK using Root KEK (one KMS call), then unwrap Slab DEK (local), then decrypt.

4. **DEK and KEK caching: time-bounded, zeroize-on-drop, memory-budgeted.**
   - **Tenant KEK cache:** unwrapped Tenant KEKs live in locked memory pages (`mlock`), protected by `ZeroizeOnDrop`, with a 15-minute idle TTL and a 60-minute absolute TTL. LRU eviction under memory pressure governed by the admission-control memory budget.
   - **Slab DEK cache:** unwrapped Slab DEKs live similarly, with a 15-minute idle TTL. Cache size bounded (default 10,000 entries, tunable) — at 80 bytes per entry that's under 1 MiB for the LRU cache itself.
   - **Explicit invalidation:** any key-rotation event or tenant-suspend operation invalidates affected cache entries immediately via a control-plane message.
   - **No disk caching.** Unwrapped keys never touch disk or network except in a controlled key-escrow flow (out of scope for this ADR).

5. **Encryption sits *above* `AlignedBuf`.** Plaintext is encrypted into a ciphertext buffer of exact size (`plaintext_len + 16B tag` per 64 KiB chunk = 65552 B per chunk). Nonces are derived from chunk ordinal and not stored per chunk. The ciphertext stream is then padded to the next 4 KiB boundary for O_DIRECT. Padding overhead is <0.1% on slab-sized writes. Placing encryption below the alignment layer corrupts page-alignment invariants and defeats the purpose of `AlignedBuf`.

6. **Slab-format encryption header.** Each slab's footer carries the encryption-relevant fields listed below; the bit-exact layout (offsets, widths, magic bytes, footer length, tail anchor) is frozen in [ADR 022 §5](022_l2_slab_format_footer_indexed_range_reads.md).
   - `slab_format_version` (u32)
   - `cipher_id` (u8, identifies which branch of ADR 019's decision tree was used)
   - `wrapped_slab_dek` (bytes, DEK wrapped by Tenant KEK)
   - `tenant_kek_id` (bytes, identifies which Tenant KEK was used so rotation is addressable)
   - `chunk_count` (u32)
   - **Slab-surface authenticator.** A 32-byte HMAC-SHA-256 over the non-ciphertext slab surface — specifically `magic_head || slab_header || offset_index || footer[0..192] || footer[224..256] || footer_length || index_length || magic_tail` — **keyed by the Tenant KEK identified by `tenant_kek_id`**. The ciphertext chunks are authenticated by their per-chunk AEAD tags (Decision §2) and are deliberately excluded from the HMAC; the HMAC field itself is excluded from its own coverage for circularity. The authenticator is verified **before** `wrapped_slab_dek` is unwrapped; a tampered footer or offset index is rejected without ever exposing the Slab DEK. HMAC-keyed by the Tenant KEK — not the Slab DEK — is load-bearing: the Slab DEK lives inside the footer in wrapped form, so keying its own authenticator on it is circular. See ADR 022 §5 for the byte ranges and rotation behavior under Tenant KEK versioning.

7. **Background plaintext-to-ciphertext migration sweeper.** A low-priority sweeper identifies plaintext slabs, re-reads them, encrypts via the standard slab-creation path (fresh DEK, chunked encryption), and retires the plaintext copy through the existing compaction machinery. Rate-limited; pausable by operator; auditable. Ships with the L2 encryption landing — a plaintext-slab cluster should be able to enable encryption and eventually reach full-ciphertext steady state without manual intervention.

## Rationale

- **Per-slab + intra-slab chunks is the only granularity that serves all four constraints.** Range-GET requires sub-slab decrypt boundaries; KMS cost bounds total DEKs; cipher safety requires bounded records per nonce space; per-record is overkill on every dimension. The intersection is per-slab DEK plus chunk-level nonces within.
- **64 KiB chunk size is a balance.** Smaller chunks (e.g., 4 KiB) multiply nonce-and-tag overhead and increase decrypt overhead on range reads that span chunks. Larger chunks (e.g., 1 MiB) waste decrypt work on small range reads. 64 KiB is a common provider min-range-read size and gives ~0.04% overhead.
- **Three-tier hierarchy matches three distinct revocation needs.** A compromised Slab DEK affects one slab. A compromised Tenant KEK affects one tenant and is recoverable by re-wrapping every Slab DEK. A compromised Root KEK is catastrophic and is exactly the key that lives in the KMS precisely because the KMS is where catastrophic keys belong.
- **Tenant KEK in the Raft state machine is the right placement.** It must be: (a) replicated, (b) versioned, (c) rotated atomically. Raft gives all three for free. Putting it in the KMS directly creates N × tenant cost on every rotation.
- **Caching in locked memory with short TTLs is the empirical sweet spot.** No-cache is a KMS-cost disaster; long-cache is a key-material lifetime disaster; zeroize-on-drop + short TTL recovers both properties. The 15-minute idle / 60-minute absolute numbers match well-studied patterns from other envelope-encrypted storage systems.
- **SSE-KMS surrender is not a compromise operators should be able to make accidentally.** Making SSE-KMS a toggle would give regulators the option to ask "why isn't it on?" and give operators a foot-gun where toggling it reads as "more secure" when in fact it surrenders the primary trust posture. The safer design is no toggle.
- **Encryption above alignment composes cleanly with the cipher decision tree.** ADR 019's primitive choice flows in here (`cipher_id` in the footer); the alignment math is the same regardless of primitive since each produces a constant ciphertext overhead per chunk.
- **Migration sweeper is the only way plaintext-to-ciphertext is actually finished.** Without it, operators enable encryption and pay the compute cost forever while plaintext slabs remain in cold storage.

## Consequences (The "Negatives")

- **KMS becomes a hard dependency for cold reads.** A slab whose Tenant KEK is not cached requires a KMS unwrap to decrypt. KMS outages therefore degrade cold-read availability. Mitigated by the Tenant KEK cache with its 60-minute absolute TTL and by hot workloads staying hot.
- **Background migration increases write load during rollout.** The sweeper re-writes every plaintext slab at roughly the provider's aggregate throughput-per-node limit. Operators rolling out encryption to a large existing cluster will see a throughput dip until migration completes. Rate-limiting is operator-tunable for this reason.
- **Key hierarchy adds deployment surface.** Three tiers means three places where misconfiguration can hurt: KMS IAM, Tenant KEK Raft state, Slab DEK footer serialization. Mitigated by exhaustive crypto-boundary integration tests.
- **Tenant KEK in Raft state grows the state machine.** Each tenant adds a wrapped Tenant KEK (≤1 KB). At 10k tenants that's ~10 MB of state machine. Trivial compared to other Raft state, but worth noting.
- **Chunk-level encryption overhead is small and uniform.** Per-chunk: 16 B authenticator tag per 64 KiB = 0.024% overhead. No per-chunk nonce storage since nonces are derived from chunk ordinal. Slab-level: footer ~200 B. Uniform across both ADR 019 cipher branches.
- **No SSE-KMS means no "enable compliance with one toggle" story.** Operators asking for SSE-KMS out of habit need to be told why it isn't there. Documentation cost, not architecture cost.
- **Rejected alternatives explicitly:**
  - *SSE-KMS as primary trust model* — rejected for provider trust surrender.
  - *SSE-KMS as compliance overlay on top of client-side envelope* — rejected for doubled testing surface, configuration ambiguity, and no cryptographic value (encrypting already-encrypted ciphertext).
  - *Per-record DEKs* — rejected for KMS fan-out and DEK-cache cardinality.
  - *Per-whole-slab encryption without chunking* — rejected; breaks range-GET.
  - *Encryption below the alignment layer* — rejected for O_DIRECT invariant corruption.
  - *Two-tier hierarchy (Root KEK → Slab DEK, no tenant layer)* — rejected because it forces every slab-DEK wrap to hit KMS directly, blowing up KMS fan-out; the tenant tier is the right amortization point.
  - *No migration sweeper (leave plaintext slabs in place)* — rejected for permanent mixed-mode.

## Open Questions

All items gate ADR 020 acceptance.

- **KMS backend abstraction.** The Root KEK lives in "the KMS," but the specific integration surface (AWS KMS, GCP KMS, Azure Key Vault, PKCS#11 HSM) is not fixed here. *Resolves via:* a separate KMS-integration design note before ADR acceptance; this ADR names the abstract operations (`wrap(key, kek_id)`, `unwrap(wrapped, kek_id)`, `rotate(kek_id)`) and leaves provider-specifics to the integration.
- **Tenant KEK rotation cadence and orchestration.** Who initiates rotation (operator-only? scheduled?), how in-flight reads against the old KEK are handled, and whether rotation re-wraps every Slab DEK or just marks the old KEK for retirement. *Resolves via:* a rotation design note before acceptance.
- **Migration sweeper throughput and ordering.** At what rate the sweeper should run, and whether it should prioritize hot slabs or cold slabs or tenant-ordered, is operationally important but benchmark-resolvable. *Resolves via:* a sweeper benchmark against a representative plaintext dataset, measured throughput and compute-cost per slab, before acceptance.
- **Footer format freeze.** ~~The slab-format encryption header fields are specified above at the field-set level but not bit-exact.~~ **Resolved by [ADR 022 §5](022_l2_slab_format_footer_indexed_range_reads.md).** The 256-byte footer is frozen bit-exact: field offsets, widths, magic_head / magic_tail, the 16-byte tail anchor, and the HMAC coverage range (`magic_head || slab_header || offset_index || footer[0..192] || footer[224..256] || footer_length || index_length || magic_tail`, keyed by the Tenant KEK identified by `tenant_kek_id`). This ADR's Decision §6 lists the encryption-relevant fields; ADR 022 §5 is the authoritative byte layout.

## References
- ADR 009: io_uring Threading Model (`AlignedBuf` alignment context)
- ADR 017: Persistent Local Index on redb (values encrypted under this hierarchy when tenant-scoped)
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization (`tenant_id` source)
- ADR 019: Cipher Selection and FIPS Posture (cipher + nonce encoding)
- ADR 021: Tenant-Aware Architecture from Day One (planned — consumes Tenant KEK per tenant)
- ADR 022: L2 Slab Format and Footer-Indexed Range Reads (planned — slab-format footer integration)
- `plans/gap/claude_answers_20260417.md` §5 (encryption) and §4 (L2)
