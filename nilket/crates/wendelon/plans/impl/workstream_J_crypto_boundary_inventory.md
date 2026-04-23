# Workstream J Crypto-Boundary Inventory

Date: 2026-04-23
Owner: `J`
Status: First repo-visible inventory

## Purpose

This inventory names the cryptographic code paths that matter for the regulated-release path. It is not a CMVP certificate, a lab boundary acceptance, or a customer-facing compliance claim.

## Build Profiles

| Profile | Cargo command | Active feature posture | Current claim |
|---|---|---|---|
| `default` | `cargo build --locked --release --bin wendelon` | default `non-fips` feature | Ordinary release profile; may use AES-256-GCM-SIV when the backend exposes it |
| `fips` | `cargo build --locked --profile fips --no-default-features --features fips --bin wendelon` | `fips` feature only | Regulated-path build profile under implementation; not FIPS-certified |

The feature gate in `src/crypto.rs` rejects simultaneous `fips` and `non-fips` features, and rejects builds with neither profile selected.

## Dependency Inventory

| Dependency | Locked version | Current use | Boundary status |
|---|---:|---|---|
| `aws-lc-rs` | `1.16.3` | AEAD, HMAC-SHA-256, SHA-256, AES key wrap, random bytes, FIPS-mode startup hook | Intended validated-module backend for the at-rest crypto boundary |
| `aws-lc-fips-sys` | `0.13.14` | FIPS-capable aws-lc-rs backend dependency | Module dependency for the `fips` build profile |
| `aws-lc-sys` | `0.40.0` | Non-FIPS aws-lc-rs backend dependency | Default-profile backend dependency |
| `blake3` | `1.8.4` | content hashes, identity hashes, default-profile audit chain hashing | Outside the FIPS boundary; FIPS audit chain uses SHA-256 |
| `rustls` / `ring` | `0.23.38` / `0.17.14` | transport TLS | Outside the current at-rest module boundary; transport-side FIPS remains a follow-on |
| `ed25519-dalek` | `2.2.0` | audit root signatures, lease signatures, transfer-token signatures | Current FIPS-profile gap unless the lab scopes it outside or the primitive moves to a validated provider |
| `scrypt` | `0.11.0` | `local-sealed` unlock-secret derivation | Current FIPS-profile gap for certified local custody unless replaced or scoped outside |

## Boundary-In Functions

| Function family | Code anchor | Provider |
|---|---|---|
| Tiered slab AEAD encryption/decryption | `src/crypto.rs` `encrypt_chunks` / `decrypt_chunks` | `aws-lc-rs` |
| FIPS cipher selection | `src/crypto.rs` `selected_cipher_id` / `detect_cipher_once` | compile-time `fips` feature |
| ADR 022 surface HMAC | `src/crypto.rs` `compute_adr022_surface_hmac` / `verify_adr022_surface_hmac` | `aws-lc-rs` HMAC-SHA-256 |
| Tenant/slab key wrapping | `src/crypto.rs` `SlabDek::wrap` and unwrap paths | `aws-lc-rs` key wrap |
| SHA-256 helpers | `src/hash/sha256.rs` | `aws-lc-rs` digest |
| FIPS startup mode check and KATs | `src/build_info.rs` `initialize_fips_runtime` | `aws-lc-rs` plus Wendelon startup KATs |

## Boundary-Out Or Open Items

| Item | Current fact | Required before regulated GA claim |
|---|---|---|
| Transport TLS | `rustls` currently depends on `ring`; ADR `026` keeps transport outside the current at-rest boundary | Follow-up transport FIPS decision or lab-approved scoping statement |
| Ed25519 signatures | Audit roots, lease tokens, and transfer tokens currently use `ed25519-dalek` | Validated-provider migration or explicit lab boundary treatment |
| `local-sealed` KDF | Unlock derivation currently uses `scrypt` | Validated KDF decision or explicit certified-profile custody restriction |
| BLAKE3 | Default content and identity hashes still use BLAKE3 outside the validated boundary | Keep confined and ensure FIPS-facing audit chain outputs `sha256` |
| CMVP state | No repo-visible certificate or lab selection exists | External lab record and allowed-claim update |

## Regression Guards

- `ci/check_fips_symbol_sweep.sh` builds the `fips` Cargo profile and rejects AES-GCM-SIV symbols/strings plus the non-FIPS audit hash tag.
- `ci/check_blake3_confinement.sh` inventories raw BLAKE3 use outside the hash wrappers.
- `tests/compliance_release_contract.rs` keeps this inventory connected to the build profile, scripts, and claim language.

## Review Triggers

- any `Cargo.lock` change touching a crypto dependency
- any new direct dependency on a crypto crate
- any new signing, hashing, KDF, AEAD, MAC, or random-byte callsite
- any change to ADR `019`, ADR `026`, or ADR `031`
- any lab-scoping decision that changes what is inside or outside the certified profile
