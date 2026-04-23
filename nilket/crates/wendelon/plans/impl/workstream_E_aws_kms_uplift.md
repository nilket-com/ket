# Workstream E AWS KMS Uplift

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `E4`
Primary Code: `src/crypto.rs`

## Purpose

Land the first real cloud custody provider behind the root-KEK provider contract without dragging AWS KMS onto the tenant-KEK hot path.

## What Landed

`aws-kms` now exists as a real `RootKekProviderConfig` variant.

It supports:

- `kind = "aws-kms"`
- explicit `region`
- explicit `key_id`
- optional `endpoint` override for test and sandbox targets
- explicit credential env names

The bootstrap contract is:

1. If `<data_dir>/.wendelon/aws-kms-root-kek.bin` already exists, Wendelon reads the persisted ciphertext envelope, verifies that it matches the configured `region` and `key_id`, and calls AWS KMS `Decrypt` to recover the 32-byte root KEK.
2. If the envelope does not exist and bootstrap mode is `GenerateOrCreate`, Wendelon generates a fresh 256-bit root KEK locally, calls AWS KMS `Encrypt`, and persists only the returned ciphertext envelope on disk.
3. After bootstrap, tenant-KEK wrap/unwrap remains local in process using the recovered root KEK, exactly like the other providers.

That is the important architectural decision: AWS KMS owns custody of the persisted root blob, but AWS KMS is not inserted into every tenant-KEK operation.

## Why This Shape

This is the right first cloud provider shape for Wendelon:

- it is a real external-custody path
- it keeps ordinary read/write paths fast
- it preserves the existing `CryptoManager` call sites
- it avoids the AWS SDK dependency forest already rejected in other seams such as L2 storage

The provider uses the AWS KMS JSON API directly with SigV4 signing over a small blocking HTTP client.

## Security And Operational Boundary

`aws-kms` is a hardening uplift, not a magic remote-HSM story.

It protects:

- persisted root-KEK material on disk
- restore/bootstrap custody by requiring access to the configured AWS KMS key

It does not protect:

- a fully compromised running host after the root KEK has been decrypted into process memory
- callers who misconfigure or lose the AWS credentials needed for bootstrap

## Current Limits

- credential sourcing is explicit env-only for the first pass
- no SDK chain resolution or instance-metadata credential provider is implemented
- root-KEK in-place rotation is still unsupported
- only AWS KMS is implemented; GCP and Azure remain follow-ons

Those limits are acceptable for `E4` because the provider is real, interoperable, and matches ADR `031`'s layering without inflating scope.

## Validation

The provider now has loopback tests covering:

- config loading of the `aws-kms` object form
- first bootstrap with a fake KMS `Encrypt`
- reopen with persisted ciphertext plus fake KMS `Decrypt`
- health reporting through fake KMS `DescribeKey`
- signed request headers carrying the configured access key and session token
