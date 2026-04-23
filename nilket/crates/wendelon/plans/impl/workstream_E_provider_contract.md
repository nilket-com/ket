# Workstream E Provider Contract

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `E1`
Primary Code: `src/crypto.rs`

## Purpose

Freeze the root-KEK provider seam before `local-sealed`, cloud KMS, or PKCS#11 work lands.

`E1` does not add a new production provider. It makes the contract explicit so later providers can plug into the same `CryptoManager` boundary without reshaping tenant-KEK lifecycle code.

## Contract

The provider surface now has three layers:

1. `RootKekProviderConfig`
   Selects the provider family from config.

2. `RootKekProviderInit`
   Supplies provider bootstrap context:
   - `data_dir`
   - `bootstrap_mode`

3. `RootKekProvider`
   Supplies runtime custody behavior:
   - `label()`
   - `health()`
   - `wrap_tenant_kek(...)`
   - `unwrap_tenant_kek(...)`
   - `rotate_root_kek()`

## Bootstrap Modes

`RootKekBootstrapMode` exists so providers can distinguish first-boot provisioning from strict enrollment:

- `GenerateOrCreate`
  The provider may create new root material if none exists yet.

- `EnrollExisting`
  The provider must attach to already-provisioned root material and fail if it is absent.

The current `NodeBootstrapConfig` path still uses `GenerateOrCreate` through `RootKekProviderConfig::build(...)`.

## Health Contract

`RootKekProviderHealth` is the minimum readiness surface:

- `ready = true`
  Provider is able to service wrap/unwrap requests.

- `ready = false`
  Provider is present but not healthy enough to trust.

The contract is intentionally small. Richer distress states can be layered later without changing the `CryptoManager` boundary.

## Rotation Contract

`rotate_root_kek()` exists in the provider trait now, but unsupported providers are allowed to return `ErrorKind::Unsupported`.

That is the right `E1` boundary:

- the interface exists now
- later providers can implement real root-key rotation
- existing code does not have to fake a rotation story it cannot yet uphold

## Current Provider Coverage

Today two provider classes exist behind the contract:

- `dev-insecure`
- `local-sealed`
- `aws-kms`

`dev-insecure` remains explicitly dev/test only:

- supports `GenerateOrCreate`
- supports `EnrollExisting` when material already exists on disk
- reports healthy once initialized
- does not support in-place root-KEK rotation

`local-sealed` is now the serious commodity baseline:

- persists encrypted Root KEK material at rest
- supports unlock via env secret or mounted secret file
- supports `GenerateOrCreate`
- supports `EnrollExisting` when sealed material already exists
- reports healthy once initialized
- does not support in-place root-KEK rotation yet

`aws-kms` is now the first cloud hardening uplift:

- persists only a KMS-encrypted Root KEK envelope on disk
- requires explicit `region`, `key_id`, and credential env configuration
- supports `GenerateOrCreate` by locally generating a Root KEK, encrypting it under AWS KMS, and persisting the ciphertext envelope
- supports `EnrollExisting` by decrypting a previously persisted ciphertext envelope through AWS KMS
- reports readiness through AWS KMS `DescribeKey`
- keeps tenant-KEK wrap/unwrap local after bootstrap instead of calling KMS on every operation
- does not support in-place root-KEK rotation yet

After `E3`, ordinary config loading defaults to `local-sealed`, while the checked-in example config and `NodeBootstrapConfig::dev()` keep `dev-insecure` only as explicit dev/test choices.

## Why This Is Enough For E1

The point of `E1` is not to finish custody. The point is to stop later custody work from changing every caller.

With this contract in place:

- `E2` can land `local-sealed`
- `E4` can land cloud KMS
- `E8` can preserve a PKCS#11 seam

without rewriting tenant-KEK wrap/unwrap call sites or `CryptoManager`.
