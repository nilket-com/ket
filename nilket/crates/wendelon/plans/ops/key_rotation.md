# Wendelon Key Rotation

This runbook covers the W4 envelope-encryption pilot:

- Root KEK wraps Tenant KEKs.
- Tenant KEKs wrap per-slab DEKs.
- Historical Tenant KEK versions stay in Raft state so pre-rotation slabs remain readable.

## Scope and Current Limits

- Tenant KEK rotation is implemented and is a Raft-committed operation.
- Startup emits a `crypto.startup` audit record with the selected cipher, provider, FIPS posture, build profile, and `mlock` availability.
- Each committed rotation emits a `crypto.tenant_kek_rotated` audit record.
- Each committed retirement emits a `crypto.tenant_kek_retired` audit record.
- Decrypt rejections emit `crypto.decrypt_rejected` with the failure stage (`footer_parse`, `tenant_kek_lookup`, `hmac`, or `aead_chunk`). If the audit sink refuses to durably persist that record, the failure is logged at `tracing::error!` and `wendelon_crypto_decrypt_audit_emit_failures_total` is incremented — a non-zero counter is a security-relevant signal that audit coverage of decrypt failures has gapped.
- A recovered cluster that predates W4 and therefore has an empty `tenant_keysets` map will auto-seed the default tenant KEK when a local leader becomes available.
- Legacy plaintext L2 slabs can be rewritten into ciphertext by the storage sweeper.
- Historical encrypted L2 slabs sealed under an older tenant KEK can now be re-wrapped by the storage sweeper onto the active tenant KEK.
- `local-sealed` is now the default serious root-KEK provider for ordinary config loading. It persists encrypted Root KEK material at `<data_dir>/.wendelon/local-sealed-root-kek.bin` and requires an unlock secret from env or a mounted file.
- `aws-kms` is now the first cloud hardening uplift. It persists a KMS-encrypted Root KEK envelope at `<data_dir>/.wendelon/aws-kms-root-kek.bin`, requires explicit AWS credentials at bootstrap, and keeps tenant-KEK wrap/unwrap local after the Root KEK has been recovered.
- `dev-insecure` remains a development placeholder, not a cloud KMS / HSM integration. It persists a local plaintext Root KEK at `<data_dir>/.wendelon/dev-root-kek.bin`, is still disabled in FIPS builds, and now exists only as an explicit dev/test choice.
- Bootstrap selects the provider explicitly via `NodeBootstrapConfig.root_kek_provider`; the node no longer hardcodes the dev provider behind the scenes.
- Key escrow is out of scope for the pilot. There is no escrow path and no recovery workflow based on escrowed plaintext keys.

## Rotation Entry Point

The preferred operator-facing rotation seam is now the Control Ring `TenantAdmin::RotateTenantKek` request.

That path:

1. Requires a cluster-scoped operator principal.
2. Reuses the same `InstallTenantKekVersion` Raft commit path as the in-process helper.
3. Refuses success if the local `CryptoManager` has not converged to the committed `active_tenant_kek_id`.

The lower-level in-process primitive remains `WendelonNode::rotate_tenant_kek(tenant_id)`.

Both paths rely on the same underlying sequence:

1. Generates a fresh Tenant KEK.
2. Wraps it under the Root KEK.
3. Commits `InstallTenantKekVersion` through Raft.
4. Lets every node converge its in-memory KEK cache from the committed state, with a full post-commit keyset sync so a transient observer failure cannot strand stale local crypto state.

Newly tiered slabs use the active Tenant KEK version after the rotation commits. Existing slabs remain readable because their footer still names the older `tenant_kek_id`, and the older wrapped Tenant KEK version remains in replicated state.
The storage sweeper now converges those older encrypted tiered slabs in the background by re-emitting them under the active tenant KEK and committing a fresh tier pointer through Raft.

W4 still does not ship a standalone CLI or HTTP admin endpoint for this primitive. The live remote control surface is the existing Control Ring tenant-admin RPC.

## Pre-Rotation Checks

Before rotating a tenant KEK:

1. Confirm the Control Ring leader is healthy and able to commit writes.
2. Confirm the target tenant is present and serving reads normally.
3. Confirm no incident response workflow is depending on manual access to plaintext keys. The pilot has no escrow fallback.

## Rotation Procedure

1. Send `TenantAdmin::RotateTenantKek { target_tenant_id }` to the current Control Ring leader from an operator-authenticated client, or invoke `WendelonNode::rotate_tenant_kek(tenant_id)` from an in-process leader-owned integration path.
2. Wait for the write to commit successfully.
3. Verify the local audit log contains `crypto.tenant_kek_rotated`.
4. Verify the tenant's `active_tenant_kek_id` in the state-machine snapshot has advanced.
5. Keep historical Tenant KEK versions intact. Do not delete prior wrapped KEKs immediately after rotation.
6. Use `TenantAdmin::Get` or `TenantAdmin::List` to inspect `tenant_kek_versions[]` and wait until an old KEK version reports `safe_to_retire = true` before planning retirement.

`rotate_tenant_kek(...)` now refuses to report success if the local node's `CryptoManager` has not converged to the committed `active_tenant_kek_id`.

## Post-Rotation Validation

Minimum validation after a rotation:

1. Write or tier a new slab for the tenant and confirm its footer carries the new `tenant_kek_id`.
2. Read at least one pre-rotation tiered slab and confirm it still decrypts successfully before the sweeper has converged it.
3. Let the sweeper run and confirm the historical tiered slab rewrites onto the new `tenant_kek_id`.
4. Verify the audit log contains `crypto.slab_dek_rewrapped` for the converged slab.
5. Confirm the audit log contains both:
   - `crypto.startup`
   - `crypto.tenant_kek_rotated`
6. Confirm `TenantAdmin::Get` reports:
   - the old KEK version still listed
   - `tiered_slab_ref_count = 0`
   - `safe_to_retire = true`

## Historical KEK Retirement

The preferred operator-facing retirement seam is now the Control Ring `TenantAdmin::RetireTenantKek` request.

That path:

1. Requires a cluster-scoped operator principal.
2. Reuses the same authoritative replicated accounting surfaced through `TenantAdmin::Get` / `List`.
3. Commits `RetireTenantKekVersion` through Raft.
4. Refuses success if the local `CryptoManager` still retains the retired KEK after commit.

Minimum retirement procedure:

1. Confirm `TenantAdmin::Get` reports `safe_to_retire = true` for the target historical KEK version.
2. Send `TenantAdmin::RetireTenantKek { target_tenant_id, tenant_kek_id }` to the current Control Ring leader from an operator-authenticated client.
3. Verify the local audit log contains `crypto.tenant_kek_retired`.
4. Verify the target `tenant_kek_id` no longer appears in `tenant_kek_versions[]`.
5. Verify the active KEK version remains present and unchanged.

The W4 test suite exercises this exact contract: post-rotation slabs seal under the new Tenant KEK while pre-rotation slabs remain readable.

## FIPS Build Verification

W4 also verified the ADR 019 FIPS posture against the release binary that ships to operators.

Use this exact sequence for a meaningful post-link check:

1. Build the FIPS profile as a release artifact, with stripping disabled so the binary remains inspectable:
   `CC=clang CXX=clang++ CARGO_PROFILE_RELEASE_STRIP=none cargo build --release --no-default-features --features fips --bin wendelon`
2. Sweep the release binary for AES-GCM-SIV symbols:
   `nm target/release/wendelon | rg -i 'aesgcmsiv|AESIV|GCM_SIV'`
3. Perform a string-level sanity check on the same artifact:
   `strings target/release/wendelon | rg -i 'aes.?gcm.?siv|AESIV'`

Operational notes:

- Do not rely on debug-profile symbol checks for this acceptance bar. Debug builds can retain unreferenced static objects and produce false positives.
- The normal release profile strips symbols, so the verification pass must override that with `CARGO_PROFILE_RELEASE_STRIP=none`.
- On this workspace, the release FIPS build was toolchain-sensitive: the default host C toolchain failed inside `aws-lc-fips-sys` during the FIPS delocation step, while the same build succeeded with `CC=clang CXX=clang++`.
- The verified W4 release artifact produced zero matches for the `nm` and `strings` checks above.
- CI now enforces the same contract through `ci/check_fips_symbol_sweep.sh` in the `fips-symbol-sweep` job.

## Plaintext-to-Ciphertext Rewrite

W4 includes a migration path for legacy plaintext L2 slabs.

- The storage sweeper rewrites legacy plaintext tiered objects into ciphertext under the normal envelope-encryption path.
- Rewrites are bounded by `SweeperConfig.max_plaintext_rewrites_per_pass`.
- Setting `max_plaintext_rewrites_per_pass = 0` pauses the rewrite path without stopping fresh encrypted tier uploads.
- Encrypted re-wraps of historical tiered slabs are bounded independently by `SweeperConfig.max_dek_rewraps_per_pass`.
- Setting `max_dek_rewraps_per_pass = 0` pauses the tenant-KEK convergence path without stopping fresh encrypted tier uploads.
- Each tier pointer carries an `is_encrypted` flag in Raft state. Once a slab has been rewritten (or freshly tiered under W4), the sweeper short-circuits without an L2 GET on subsequent passes, so a fully-migrated fleet pays no per-scan rewrite cost. Legacy `tiered_slabs_v1` rows are migrated to `tiered_slabs_v2` on first node start with `is_encrypted = false`; the sweeper will GET each one once, encrypt, and stamp the truth back.

Operational guidance:

1. Leave rewrites enabled after rotation if legacy plaintext L2 data still exists.
2. Leave DEK re-wrap enabled after rotation until historical encrypted tiered slabs have converged onto the active tenant KEK.
   TenantAdmin retirement accounting is only authoritative once `tenant_kek_accounting_complete = true`.
3. If object-store bandwidth is constrained, reduce the per-pass rewrite counts or the upload byte cap. In W4, `max_upload_bytes_per_sec` throttles the PUT side only; rewrite-path L2 GET traffic remains unthrottled.
4. Resume rewrites after maintenance by restoring a non-zero rewrite count.

## Scope Boundaries

- W4 encrypts L2 tier objects and wrapped key material. It does not encrypt local L1 slab files or the plaintext rehydration cache written after an L2 decrypt.
- E7 now zeroizes:
  - KEK/DEK material held in the dedicated crypto key wrappers
  - unlock-secret buffers
  - selected transient plaintext buffers that exist only to be re-encrypted during tier upload / KEK re-wrap / retention rewrite
- E7 does **not** zeroize every plaintext record payload in the process. Read-path results, object-store buffers, and rehydrated local slab contents still use ordinary payload buffers.

## Rollback and Recovery

Rollback is forward-only.

- If a rotation is committed and later judged undesirable, rotate again to a new Tenant KEK version.
- Do not retire historical Tenant KEK versions until every slab that depends on them has been rewritten or retired. W4 now computes this accounting signal through replicated tier metadata and enforces retirement through an explicit operator action.
- If a rewrite of a legacy plaintext slab fails, the sweeper retries on the next pass. The original pointer stays authoritative until the new ciphertext pointer commits.
- If an encrypted DEK re-wrap fails, the sweeper retries on the next pass. The original tier pointer stays authoritative until the replacement pointer commits.

## No-Escrow Posture

The W4 pilot has an explicit no-escrow posture.

- Wendelon does not persist plaintext Root KEKs, Tenant KEKs, or Slab DEKs to disk.
- The `dev-insecure` provider is an explicit development exception: it persists a local plaintext Root KEK at `<data_dir>/.wendelon/dev-root-kek.bin`. The default serious path is `local-sealed`, which encrypts the persisted Root KEK blob at rest. Neither path is an escrow mechanism.
- Wrapped Tenant KEKs live in Raft state.
- Wrapped Slab DEKs live in slab footers.
- Loss of every valid Root KEK unwrap path is unrecoverable for encrypted data in this pilot.

Cloud-KMS / HSM integration and any future escrow policy remain separate workstreams.

For the first cloud path, AWS KMS integration is now real. Additional providers and escrow policy remain separate workstreams.
