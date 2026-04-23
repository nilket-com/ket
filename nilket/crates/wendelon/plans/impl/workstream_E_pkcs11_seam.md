# Workstream E: PKCS#11 Seam

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `E`

## Objective

Preserve a stable enterprise-HSM / regulated-uplift seam without pretending Wendelon ships a working PKCS#11 backend today.

## What Landed

- `RootKekProviderConfig` now includes a real `pkcs11` variant.
- The config surface is explicit and future-proof:
  - `library_path`
  - `token_label`
  - `key_label`
  - `pin_env`
  - optional `slot`
- Relative `library_path` values are normalized against the config file directory, matching the rest of the bootstrap config behavior.
- Validation enforces:
  - `library_path` exists
  - `token_label`, `key_label`, and `pin_env` are non-empty
  - `pin_env` is present in the process environment
- Provider construction currently returns `ErrorKind::Unsupported` with an explicit “provider seam only” message.

## Why This Is Enough For E8

E8 was never supposed to ship vendor-specific HSM support.

The real requirement was:

- do not reshape the provider contract later
- do not let future PKCS#11 support invent a one-off bootstrap/config path
- make the unsupported status explicit instead of ambiguous

That is now true.

## What Is Deliberately Deferred

- actual `dlopen` / module loading
- slot/session management
- PIN login / re-login policy
- key discovery and object-handle caching
- health/readiness against a live token
- vendor-specific quirks and lab qualification

## Evidence

- config-loader proof: `tests/config_loader.rs::loads_pkcs11_root_kek_provider_from_config`
- unsupported-build proof: `src/crypto.rs::tests::pkcs11_provider_seam_is_configurable_but_build_is_unsupported`
