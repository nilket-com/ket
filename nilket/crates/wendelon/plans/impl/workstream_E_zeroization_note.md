# Workstream E: Zeroization And Sensitive-Buffer Hygiene

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `E`

## Objective

Tighten the crypto stack so short-lived key material and selected transient plaintext buffers are explicitly scrubbed, while documenting the parts of the storage/data path that still operate on ordinary application payload buffers.

## What Landed

- `KeyMaterialBytes` remains the base 32-byte secret wrapper and now has deterministic drop-time scrub coverage.
- `LockedKeyMaterial` now routes destruction through a single `scrub_for_drop()` path that:
  - zeroizes the in-memory key bytes
  - calls `munlock` when pinning succeeded
  - prevents double-`munlock` on later drop
- AWS KMS credential state now uses `Zeroize` / `ZeroizeOnDrop` so the loaded secret access key and optional session token are scrubbed when the provider client is dropped.
- AWS KMS root-KEK decrypt now zeroizes the base64-decoded plaintext buffer after copying it into `KeyMaterialBytes`.
- AWS SigV4 signing-key derivation now treats the derived signing key as `KeyMaterialBytes` and zeroizes the `AWS4<secret>` staging buffer.
- Selected short-lived plaintext staging buffers now use `Zeroizing<Vec<u8>>`:
  - local sealed-slab bytes during tier upload
  - decrypted tiered-slab plaintext during KEK re-wrap
  - decrypted tiered-slab plaintext and extracted frames during retention rewrite

## Deterministic Evidence

- `src/crypto.rs::key_material_bytes_drop_zeroizes_memory`
- `src/crypto.rs::locked_key_material_scrub_zeroizes_inner_bytes`

These tests intentionally inspect memory after scrub on controlled internal types. They are not a claim that every secret-bearing allocator page in the process is observable or provably cleared.

## Explicit Boundaries

This work does **not** claim universal plaintext zeroization across Wendelon.

Still out of scope for E7:

- record payload buffers intentionally returned to callers
- ordinary `Bytes` / `Vec<u8>` values returned directly from the object-store seam
- rehydrated local slab contents on disk
- general application/query payload memory outside the crypto and rewrite-specific paths above

The practical contract is narrower and honest:

- KEK/DEK material is wrapped in scrubbed key types
- unlock secrets are loaded into zeroizing buffers
- selected transient plaintext buffers that exist only to be re-encrypted are scrubbed on drop
- broader user-data plaintext handling remains a separate hardening track

## Why This Cut

This is the right founder-stage / pilot-stage bar:

- it materially reduces the lifetime of actual secret material
- it does not require invasive API churn across the whole storage and query stack
- it preserves a clean seam for stronger future hardening without overclaiming current guarantees
