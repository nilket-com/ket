# Workstream D — Encryption at Rest

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phase 4)
**Status:** First pass
**Subsystems:** `src/storage/core.rs`, `src/consensus/raft_storage.rs`, `src/storage/l2.rs`, new `src/crypto/` module
**Depends on:** Workstream A (integration gate); ADR must precede implementation

---

## Problem Statement

There is no encryption at rest anywhere in the system. Slab files on
NVMe, WAL pages, snapshots, and L2 objects in S3 are all written in the
clear. `grep` across `src/` for encryption primitives returns only
`tls.rs` (in-transit encryption). This is the single most disqualifying
gap for regulated industries (finance, healthcare, government) and was
flagged unanimously by all three gap reviewers.

Adding encryption at rest is not a bolt-on — it touches every persistence
path and must integrate with the io_uring + O_DIRECT write pipeline,
the WAL record format, the snapshot serialization, and the L2 upload
path. It requires a KMS integration for key management and must support
key rotation without data rewrite.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| Crypto module (new) | `src/crypto/` | KMS trait, envelope encryption, key cache |
| Slab encryption | `src/storage/core.rs` | Encrypt before `O_DIRECT` write, decrypt after read |
| WAL encryption | `src/consensus/raft_storage.rs` | Encrypt WAL records |
| Snapshot encryption | `src/consensus/raft_storage.rs` | Encrypt snapshot bytes before persist |
| L2 encryption | `src/storage/l2.rs` | Client-side envelope or SSE-KMS |
| Config | `src/config.rs` | KMS config section |

**Boundary note:** Per-tenant key isolation is scoped here as
preparation, but full multi-tenancy routing is Workstream I.

---

## Ordered Implementation Steps

### D.0 — ADR (blocks all implementation)

| # | Step | Details |
|---|------|---------|
| D.0.1 | Write ADR: encryption-at-rest design | Envelope encryption scheme. Key hierarchy: DEK per slab (or per WAL segment), KEK in KMS. Decide AES-256-GCM vs. XChaCha20-Poly1305. Address O_DIRECT alignment (encryption output must remain page-aligned for `AlignedBuf`). Address nonce management (random per record, or counter-based?). Address crypto-erase (delete KEK to render all DEKs unrecoverable). Address key rotation (re-wrap DEKs with new KEK, no data rewrite needed). |
| D.0.2 | Review ADR with architect | Confirm integration points, performance budget, KMS provider priority |

### D.1 — KMS Abstraction

| # | Step | Target | Details |
|---|------|--------|---------|
| D.1.1 | Define `KmsProvider` trait | `src/crypto/kms.rs` (new) | `generate_dek()`, `encrypt_dek(dek, kek_id)`, `decrypt_dek(wrapped_dek, kek_id)`, `rotate_kek(old_id) -> new_id` |
| D.1.2 | Local-dev KMS implementation | `src/crypto/kms.rs` | Deterministic key derivation from a master secret for dev/test. Clearly marked unsafe for production. |
| D.1.3 | AWS KMS implementation | `src/crypto/aws_kms.rs` (new) | Wrap/unwrap via `aws-sdk-kms`. |
| D.1.4 | GCP Cloud KMS implementation | `src/crypto/gcp_kms.rs` (new) | Optional, behind feature flag. |
| D.1.5 | HashiCorp Vault Transit implementation | `src/crypto/vault_kms.rs` (new) | Optional, behind feature flag. |
| D.1.6 | DEK cache | `src/crypto/kms.rs` | Cache unwrapped DEKs in memory with TTL to avoid KMS round-trip on every read. Zeroize on drop. |

### D.2 — Slab Record Encryption

| # | Step | Target | Details |
|---|------|--------|---------|
| D.2.1 | Generate DEK on slab creation | `src/storage/core.rs` | When a new slab is opened for writing, generate a DEK via KMS. Store wrapped DEK in slab header. |
| D.2.2 | Encrypt records before write | `src/storage/core.rs` | After serialization, before `O_DIRECT` append. Encryption output must maintain page alignment. Prepend nonce to ciphertext (or use a counter derived from record offset). |
| D.2.3 | Decrypt records after read | `src/storage/core.rs` | On `read_record`, unwrap DEK (from cache or KMS), decrypt ciphertext, then verify blake3 checksum on plaintext. |
| D.2.4 | Slab header format for wrapped DEK | `src/storage/core.rs` | Add a header block at the start of each slab file containing: format version, wrapped DEK, KEK ID, encryption algorithm indicator. |
| D.2.5 | Unit test: encrypted write + read round-trip | `src/storage/core.rs` tests | Write records with encryption, read back, verify plaintext matches. |
| D.2.6 | Unit test: raw slab bytes are not plaintext | `src/storage/core.rs` tests | Write known data, read raw file bytes, assert they don't contain the known plaintext. |

### D.3 — WAL and Snapshot Encryption

| # | Step | Target | Details |
|---|------|--------|---------|
| D.3.1 | Encrypt WAL records | `src/consensus/raft_storage.rs` | Encrypt each WAL record payload before append. Use a WAL-segment-level DEK (rotated on each new segment). |
| D.3.2 | Encrypt snapshot bytes | `src/consensus/raft_storage.rs` | Encrypt the snapshot blob before `persist_snapshot` (Workstream B). Use a snapshot-specific DEK. |
| D.3.3 | Recovery with encrypted WAL + snapshot | `tests/` | Write encrypted entries, crash, recover. Verify state machine is correct. |

### D.4 — L2 Object Encryption

| # | Step | Target | Details |
|---|------|--------|---------|
| D.4.1 | Client-side envelope encryption for L2 uploads | `src/storage/l2.rs` | Encrypt slab bytes before `put()`. Store wrapped DEK as S3 object metadata. |
| D.4.2 | SSE-KMS support as alternative | `src/storage/l2.rs` | For deployments that prefer server-side encryption. Configure via TOML. |
| D.4.3 | L2 read decryption | `src/storage/l2.rs`, `src/storage/manager.rs` | Decrypt after `get()` / `get_range()`. Unwrap DEK from object metadata. |

### D.5 �� Key Rotation and Crypto-Erase

| # | Step | Target | Details |
|---|------|--------|---------|
| D.5.1 | KEK rotation: re-wrap all DEKs with new KEK | `src/crypto/` | Background task that walks slab headers and re-wraps DEKs. No data rewrite needed. |
| D.5.2 | Crypto-erase: delete KEK from KMS | `src/crypto/` | Operator command that deletes a KEK, rendering all data encrypted with that key unrecoverable. For tenant offboarding. |
| D.5.3 | Per-tenant key isolation | `src/crypto/` | Associate KEK IDs with tenant namespaces. Prepare the mapping but don't enforce routing (that's Workstream I). |

---

## Test Plan

| Test | Type | What it proves |
|------|------|----------------|
| Encrypted write + read round-trip | Unit | Encryption/decryption pipeline works |
| Raw bytes not plaintext | Unit | Data is actually encrypted on disk |
| Encrypted WAL recovery | Integration | Recovery works through encryption layer |
| L2 encrypted upload + download | Integration | S3 objects are encrypted, can be read back |
| Key rotation (re-wrap) | Integration | Old data readable with new KEK |
| Crypto-erase | Integration | After KEK deletion, data is unrecoverable |
| KMS unavailable during read | Unit | Cached DEK allows reads; cache miss fails gracefully |

---

## Rollout and Migration Concerns

- **Existing unencrypted data.** Clusters upgrading from pre-encryption
  code have plaintext slabs, WAL, and L2 objects. The system must support
  a "mixed" mode where old slabs have no encryption header and new slabs
  do. Read path must detect the absence of encryption header and skip
  decryption. A background migration tool to re-encrypt existing data
  may be needed but is not in scope for this workstream.
- **Performance impact.** AES-256-GCM on modern CPUs with AES-NI is
  ~1-2 cycles/byte. For a 64 MiB slab that's ~30-60ms. Benchmark to
  confirm this is acceptable within the io_uring pipeline. If not,
  consider encrypting at a larger granularity (per-page rather than
  per-record).
- **O_DIRECT alignment.** Ciphertext size must remain page-aligned for
  `AlignedBuf`. AES-GCM adds a 16-byte tag; if records are not
  page-aligned, padding is needed. This is the trickiest integration
  point and must be addressed in the ADR.

---

## Failure Modes and Rollback

- **KMS unreachable.** The DEK cache provides a grace period for reads.
  Writes of new slabs fail if a DEK cannot be generated. The node should
  enter a degraded state, not crash. Surface via metrics.
- **Corrupt encryption header.** If a slab's wrapped DEK is corrupt, the
  slab is unreadable. The repair path is to restore from a replica that
  has a valid copy. No local recovery is possible.
- **Key rotation interrupted.** If re-wrapping is interrupted, some slabs
  have old wrapped DEKs and some have new. This is fine — the old KEK
  must remain available until re-wrapping is complete. Track progress in
  a manifest.
- **Rollback after encryption enabled.** Once data is encrypted, rolling
  back to pre-encryption code means that data is unreadable. Rollback
  requires a migration tool to decrypt in place. This is an inherent
  risk of enabling encryption.

---

## Exit Criteria

- `grep` for known plaintext in slab files, WAL, and snapshots returns
  nothing after encryption is enabled.
- Encrypted data survives node restart.
- Key rotation test passes (re-wrapped data still readable).
- L2 objects are encrypted (client-side or SSE-KMS).

---

## Open Questions

1. **AES-256-GCM vs. XChaCha20-Poly1305?** GCM has hardware acceleration
   (AES-NI) but requires careful nonce management (96-bit, no reuse).
   XChaCha20 has a 192-bit nonce (safe to randomize) but no hardware
   accel. For per-record encryption with potentially billions of records,
   nonce collision risk with GCM needs analysis.
2. **Encryption granularity:** Per-record, per-page, or per-slab? Per-record
   is finest-grained (supports per-tenant keys at record level) but has
   the highest overhead and the most complex alignment story. Per-slab is
   simplest but prevents per-tenant key isolation within a slab.
3. **O_DIRECT alignment with encryption.** Does the encryption layer sit
   above or below the `AlignedBuf` page alignment? If above, ciphertext
   must be padded to page boundaries. If below, the alignment logic must
   account for nonce + tag overhead.
4. **DEK cache TTL.** How long to cache unwrapped DEKs? Too short = KMS
   round-trips on every read. Too long = larger window of exposure if
   memory is compromised. Need to balance performance and security.
5. **FIPS interaction.** If FIPS mode (Workstream E) is enabled, the
   crypto primitives must come from a FIPS-validated module. Does
   `aws-lc-rs` (FIPS) support the chosen cipher? Coordinate with
   Workstream E.
6. **Mixed-mode migration tool.** Is a background re-encryption tool for
   existing plaintext data in scope for this workstream, or deferred to
   a separate effort?
