# W4 — Crypto & Envelope Encryption

**Effort band:** M (4–8 eng-weeks)
**Predecessors:** W1 (redb for key material, AsyncFileOps)
**Folds:** none

End of W4, all L2 bytes and all at-rest keyed state can be encrypted under the three-tier key hierarchy with per-slab DEKs, 64 KiB chunked encryption, and Tenant KEK rotation. W7 (L2) and W9 (audit signatures) consume W4's primitives directly; W5 (tenant-aware) consumes the Tenant KEK layer.

## 1. ADRs consumed

- **ADR 019 (Cipher Selection and FIPS Posture).** `aws-lc-rs` backend; default profile uses AES-256-GCM-SIV when available and injective-nonce AES-256-GCM otherwise. FIPS profile is plain-GCM only (SIV compiled out per ADR 026 §2a). Uniform nonce strategy across both branches: `version(u32be) || chunk_index(u64be)`.
- **ADR 020 (L2 Envelope Encryption and DEK Lifecycle).** Three-tier hierarchy Root KEK → Tenant KEK → **per-slab DEK**, with 64 KiB intra-slab chunked encryption and a 48-byte fixed-layout AAD. Client-side envelope only, no SSE-KMS.
- **ADR 029 (Identity vs Locality).** Consumed as a structural constraint: the AAD and nonce schemes must not acquire dependencies on record `Identity`, which would introduce per-record AAD variance. Slab-scoped addressing (SlabId, chunk_index) is unaffected.

## 2. Deliverables

1. **`aws-lc-rs` integration** with a two-branch cipher-selection decision tree. The branch taken is a compile-time fact in the FIPS profile (SIV absent) and a boot-time fact in the default profile (SIV feature-probed once).
2. **Three-tier key hierarchy.** Root KEK in cloud KMS (or on-prem HSM via PKCS#11) — Wendelon never sees it plaintext; Tenant KEK wrapped by Root KEK and stored in Raft state; Slab DEK wrapped by Tenant KEK and stored in the slab footer. One Slab DEK per slab. No DEK is ever reused across slabs.
3. **64 KiB intra-slab chunked encryption.** Fixed 64 KiB plaintext chunks; `nonce = version(u32be) || chunk_index(u64be)` derived from chunk ordinal within slab (not stored per chunk); 16-byte per-chunk AEAD tag.
4. **48-byte fixed-layout AAD** per chunk: `tenant_id(16) || slab_id(16) || chunk_index(u64be,8) || slab_format_version(u32be,4) || cipher_id(u8,1) || reserved(3 zero)`. Byte-exact layout — not CBOR, not length-prefixed.
5. **Per-slab DEK lifecycle** — fresh DEK at slab creation, wrapped by the active Tenant KEK, stored in the slab footer's encryption header (`wrapped_slab_dek`, `tenant_kek_id`, `cipher_id`, `chunk_count`, slab-surface HMAC keyed by Tenant KEK).
6. **DEK/KEK caching** per ADR 020 §4: Tenant KEK in `mlock`'d memory with ZeroizeOnDrop, 15-min idle / 60-min absolute TTL; Slab DEK with 15-min idle TTL and a bounded LRU; explicit invalidation on rotation or tenant suspend; no disk caching.
7. **`slab_max_bytes` compile-time ceiling at `2^35` bytes (32 GiB).** Enforced at config-load time, not documented-only. Load-bearing for the plain-GCM branch's per-DEK data bound (2× safety margin under NIST SP 800-38D's `2^36`-byte limit).
8. **Key rotation procedure** documented in `plans/ops/key_rotation.md` and exercised by integration test; Tenant KEK rotation is a Raft-committed operation.
9. **Plaintext-to-ciphertext migration sweeper** (ADR 020 §7) that re-reads plaintext slabs, encrypts via the standard slab-creation path, and retires the plaintext copy through compaction. Rate-limited, pausable, auditable.

## 3. Acceptance criteria (ADR-referential)

- (ADR 019) Default-build cipher-selection branch is taken by a single code path at startup; the selected cipher appears in a startup audit record (ADR 025 emission).
- (ADR 019) FIPS-build binary does not link the SIV primitive — confirmed by a post-link symbol check.
- (ADR 019 §2 acceptance bar) **Injective nonce encoding test.** Distinct `chunk_index` inputs under one DEK produce distinct nonces across a fuzzed input space. Runs to 100M samples in the weekly ladder.
- (ADR 019 §3 acceptance bar) **Per-slab DEK invariant (load-bearing).** DEKs are never reused across slabs. Enforced by construction — a slab-create path that would reuse a DEK is a compile-time surface violation, not a runtime check.
- (ADR 019 §1 / ADR 020 §2) **Per-DEK data bound structural, not probabilistic.** `slab_max_bytes ≤ 2^35` is enforced at config load; a config that exceeds the ceiling refuses to boot.
- (ADR 019 §4) **Reserved version byte.** `version = 0` today; any non-zero observed version is a decrypt error.
- (ADR 020 §2) **AAD bit-exactness.** Canonical 48-byte AAD layout is reproducible across library versions and language toolchains — verified by a golden-AAD test fixture.
- (ADR 020 §6) **Slab-surface HMAC** is verified *before* `wrapped_slab_dek` is unwrapped; a tampered footer is rejected without exposing the Slab DEK.
- (ADR 020 decision §1) L2 bytes on the object store are ciphertext-only under the three-tier hierarchy; a plaintext byte in any L2 object is a test failure.
- (ADR 020 decision §1) SSE-KMS code path does not exist in the binary (negative test: grep for KMS-SSE symbol returns zero matches outside comments).

## 4. Explicit non-goals

- FIPS 140-3 CMVP module boundary + lab engagement. **W10.**
- Tenant-lifecycle-driven KEK creation/destroy. **W5.**
- L2 slab format and upload lifecycle. **W7.**

## 5. Open items

- **ADR 019 Open Question — SIV availability in `aws-lc-rs`.** Verify at W4 kickoff; if SIV lands in `aws-lc-rs` after W4, close by running the default-build cipher-selection test suite end-to-end.
- **ADR 020 Open Question — key escrow.** Explicitly out of scope for pilot; closed by a documented "no-escrow" posture in `plans/ops/key_rotation.md`.

## 5.1 Implementation Notes

- **FIPS post-link verification must use a release artifact.** Debug-profile symbol sweeps are not load-bearing for ADR 019 because unoptimized static linking can retain unreferenced AWS-LC objects and produce false positives.
- **Release-profile symbol sweeps need stripping disabled.** The project's normal release profile sets `strip = true`, so the verification command uses `CARGO_PROFILE_RELEASE_STRIP=none` to keep the release binary inspectable.
- **Toolchain sensitivity observed during W4 verification.** On this workspace, `cargo build --release --no-default-features --features fips --bin wendelon` under the default host C toolchain failed inside `aws-lc-fips-sys` during the FIPS delocation step. The same release build succeeded when forced to Clang with `CC=clang CXX=clang++`.
- **Verified W4 acceptance path.** The working ADR 019 verification sequence was:
  `CC=clang CXX=clang++ CARGO_PROFILE_RELEASE_STRIP=none cargo build --release --no-default-features --features fips --bin wendelon`
  followed by
  `nm target/release/wendelon | rg -i 'aesgcmsiv|AESIV|GCM_SIV'`
  and
  `strings target/release/wendelon | rg -i 'aes.?gcm.?siv|AESIV'`.
  On the resulting release binary, both checks returned zero matches.
- **Root KEK bootstrap boundary is now explicit.** `NodeBootstrapConfig` carries a `root_kek_provider` selector, startup audit records include the chosen provider, and the runtime no longer hardcodes the dev root KEK path in `WendelonNode::bootstrap`.
- **The dev placeholder provider no longer bakes its Root KEK into the binary.** `dev-insecure` now creates a per-data-dir local Root KEK file at `<data_dir>/.wendelon/dev-root-kek.bin`, which keeps dev restarts readable while remaining explicitly outside the production custody model.
- **Recovered pre-W4 clusters now self-heal into W4 state.** If a recovered state machine snapshot/log predates `tenant_keysets`, the local leader seeds the default tenant KEK automatically instead of requiring an operator-side bootstrap workaround.
- **Committed Tenant KEK rotations converge by snapshot, not just edge-triggered callbacks.** The Raft apply path still fires per-rotation observer callbacks, but now follows them with a full committed `tenant_keysets` sync so a transient observer failure cannot leave a node's local crypto state permanently stale.
- **Conflicting `tenant_kek_id` reuse is rejected.** The rotation path now mints non-zero ids that are unused for the target tenant, and the Raft state machine rejects a conflicting duplicate id instead of silently overwriting historical wrapped KEK bytes.
- **The W4 rotation seam is still in-process only.** `WendelonNode::rotate_tenant_kek(...)` is the primitive, but W4 does not yet lift that into a standalone CLI/admin RPC surface.
- **W4's at-rest boundary is L2, not every plaintext landing zone.** L2 tier objects and wrapped keys are encrypted; local L1 slab files, rehydration cache files, and decrypted plaintext buffers remain plaintext in this workstream.
- **The rewrite bandwidth cap is upload-only.** `max_upload_bytes_per_sec` limits sweeper PUT traffic; rewrite-path L2 GET traffic remains unthrottled in W4.
- **`InstallTenantKekVersion` follows the existing epoch-0 retention bucket.** It shares the same WAL-retention semantics as other epoch-agnostic control entries such as `TierSlab`.
- **Tier-pointer refreshes are part of the W4 rewrite contract.** Re-encryption and URI/hash repair can commit a second `TierSlab` entry for an existing `slab_id`; the Raft state machine updates the pointer in place and deliberately re-fires the tier observer so runtime readers pick up the new L2 object.
- **Nonce ladder seam is explicit.** The per-PR unit test keeps the 100k sampled-space check in-tree, and the weekly 100M run now has a dedicated explicit harness via `ci/check_w4_nonce_ladder.sh` plus the scheduled `.github/workflows/crypto-weekly.yml` workflow.
- **FIPS symbol verification is now automated.** The manual ADR 019 sweep remains documented for operators, and CI now enforces the same release-build `nm`/`strings` contract via `ci/check_w4_fips_symbol_sweep.sh`.
- **Node-local crypto audits use the same sink path as Raft-materialized audits.** `crypto.startup` and `crypto.decrypt_rejected` are emitted directly because they are not committed-log events, but they still flow through the shared `AuditSink` used by the Raft audit materializer.
- **Sweeper steady-state short-circuit.** Each `TieredSlab` / `TieredSlabPointer` now carries an `is_encrypted` flag. The plaintext-rewrite sweeper skips already-encrypted pointers without a GET, so once a fleet has fully migrated, the rewrite pass costs O(0) per scan. The redb-backed local index bumped its tiered-slabs schema from `tiered_slabs_v1` → `tiered_slabs_v2` (`payload_hash[32] || flags_byte || l2_uri_utf8`); a one-shot v1→v2 migration runs on `PersistentLocalIndex::open` and stamps `is_encrypted = false` on legacy rows, which the sweeper will then GET-and-rewrite once before the short-circuit takes hold.
- **Decrypt-audit emit failure is loud and metered.** `crypto.decrypt_rejected` audit emit failures now log at `tracing::error!` level and bump `wendelon_crypto_decrypt_audit_emit_failures_total`. The audit sink refusing a decrypt-rejection record is a security-relevant signal, not a no-op.
- **Per-attempt rewrite key.** The plaintext-to-ciphertext rewrite path appends a UUID v4 suffix to each L2 PUT key (`crypto/slab_<id>_<uuid>.wnd`). A leader-handoff retry never reuses the same key as an earlier in-flight upload, so two concurrent rewrite attempts cannot stomp each other's object-store state.
- **Upload-rate cap is unclamped to scan interval.** `rate_limit_upload` no longer clamps the per-upload sleep to `scan_interval`. A large slab at a tight cap sleeps for the full `bytes / cap` duration, so the sweeper's average outbound rate stays at or below the configured cap rather than silently exceeding it for one-scan bursts.
- **Nonce ladder is structurally sampled, not a tautology.** `assert_nonce_encoding_matches_contract` now drives chunk-index inputs through a Knuth-period u64 LCG plus boundary inputs (`0`, `1`, `u64::MAX`, `1<<32`, `1<<63`, alternating-bit), and asserts per-sample identity round-trip. A bounded-cap HashSet (`NONCE_LADDER_HASHSET_CAP = 1_000_000`) catches collisions in the sampled subspace without growing memory linearly with the 100M weekly run.

## 6. Test harness tier

- **Unit:** chunk-ordinal nonce encoder; AAD fixed-width layout; DEK-wrap/unwrap round-trip; slab-surface HMAC.
- **Property:** injective-nonce-per-DEK at scale; `slab_max_bytes` ceiling enforcement; AAD bit-exactness across toolchains.
- **Integration:** Tenant KEK rotation on a running cluster; post-rotation read of pre-rotation slabs; plaintext-to-ciphertext sweeper end-to-end.
- **Simulator:** W10 exercises rotation mid-chaos-scenario.

## 7. Phasing (internal)

- **W4.a (S):** `aws-lc-rs` + cipher selection + chunk-ordinal nonce encoder.
- **W4.b (M):** Three-tier hierarchy + AAD layout + slab footer encryption header + slab-surface HMAC.
- **W4.c (S):** Rotation procedure + plaintext-to-ciphertext sweeper + operational docs.

## 8. Identity/Locality impact (ADR 029 addendum)

- **AAD layout is unchanged.** The 48-byte AAD (`tenant_id(16) || slab_id(16) || chunk_index(u64be,8) || slab_format_version(u32be,4) || cipher_id(u8,1) || reserved(3)`) does not gain Identity bytes. Identity lives in the plaintext slab body as a per-record header field, not in the AAD — keeping AAD a function of slab topology, not record content.
- **Nonce scheme is unchanged.** `nonce = version(u32be) || chunk_index(u64be)` stays chunk-ordinal. Invariant I-3 is upheld: Identity is a storage-layer concern, not a crypto-layer concern.
- **FIPS build profile boundary.** Record `Identity` remains BLAKE3-256 across build profiles. Under the FIPS profile (ADR 026, productionized in W10), that helper stays outside the validated module boundary, matching ADR 026's existing treatment of content-addressed hashes. The slab crypto format does not gain an `identity_algorithm_tag`, and no profile-driven re-identification sweep is introduced by W4.
- **Slab DEK wrap-path does not leak Identity.** `wrapped_slab_dek` is addressed by `SlabId`, not by record `Identity`. Slab-surface HMAC keyed by Tenant KEK is slab-scoped; adding any Identity-derived bytes would silently break the rotation contract. Enforced by a negative test.
- **Plaintext-to-ciphertext sweeper.** Sweeper reads plaintext slab body, re-encrypts, retires plaintext. Because Identity lives in the plaintext slab body (not in AAD), sweeper has no Identity-touching responsibility and this invariant should be documented as a sweeper non-concern.
- **Open item added.** Document the module-boundary split explicitly: record-identity hashing remains outside the CMVP scope, while the slab AEAD / KEK / HMAC machinery stays inside. W10.a owns the wording and verification.
