# Workstream E Local-Sealed Baseline

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `E2`
Primary Code: `src/crypto.rs`

## Purpose

Establish a serious commodity-default Root KEK provider without requiring cloud KMS or HSM hardware.

`local-sealed` is the first non-`dev-insecure` custody path in the codebase.

## Provider Shape

`local-sealed` persists Root KEK material at:

- `<data_dir>/.wendelon/local-sealed-root-kek.bin`

That file is not plaintext. It stores:

- fixed magic and version
- random KDF salt
- random AEAD nonce
- AES-256-GCM ciphertext+tag for the 32-byte Root KEK

## Unlock Modes

Exactly one unlock mode must be configured:

- `unlock_secret_env`
- `unlock_secret_file`

That covers the two baseline startup stories from ADR `031`:

- operator-supplied secret at boot
- platform secret injection via mounted file

## KDF And At-Rest Boundary

The unlock secret is stretched with `scrypt` and a per-file random salt to derive the local wrapping key used for the persisted Root KEK blob.

The security statement remains the ADR `031` baseline:

- protects against disk theft, backup leakage, and accidental disclosure of stored key material
- does not claim protection against hostile-host or full-root compromise

## Bootstrap Behavior

`GenerateOrCreate`:

- if the sealed file exists, decrypt and use it
- if it does not exist, mint a fresh Root KEK, seal it locally, and persist it

`EnrollExisting`:

- requires the sealed file to already exist
- fails closed if the file is absent or the unlock secret is wrong

## Validation Landed

The code now has targeted coverage for:

- env-secret bootstrap and restart reuse
- file-secret bootstrap and restart reuse
- wrong-secret rejection
- config-loader parsing for the `local-sealed` object form

## Known Limits

- no in-place Root KEK rotation yet
- no cloud-KMS provider yet
- no PKCS#11 provider yet
