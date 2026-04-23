# Workstream E Production Gating

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `E3`
Primary Code: `src/crypto.rs` `src/config.rs` `src/node.rs`

## Purpose

Make `dev-insecure` an explicit dev/test escape hatch instead of the silent custody default.

## Policy Landed

The code now draws three distinct paths:

- config-loader default: `local-sealed`
- checked-in dev example config: explicit `dev-insecure`
- programmatic dev helper: `NodeBootstrapConfig::dev()` still uses explicit `dev-insecure`

That is the important behavioral correction:

- serious config consumers no longer silently inherit `dev-insecure`
- dev/test bring-up still works, but only through explicit choices

## Default Behavior

When `[crypto]` omits `root_kek_provider`, config loading now defaults to:

- `kind = "local-sealed"`
- `unlock_secret_env = "WENDELON_ROOT_KEK_SECRET"`

That makes the serious commodity baseline the default custody posture for ordinary config files.

## Dev-Insecure Position

`dev-insecure` still exists, but it is now deliberately narrow:

- explicit in the checked-in example config
- explicit in `NodeBootstrapConfig::dev()`
- still disabled in FIPS builds
- emits a startup warning when bootstrapped

The example config is intentionally dev-only and says so.

## Why This Is Enough For E3

This closes the most dangerous gap without inventing a fake deployment-profile system:

- the bad provider is no longer the silent default
- the serious provider is now the ordinary config default
- dev convenience remains available without pretending it is production-safe

## Remaining Limits

- non-FIPS builds still allow an explicitly selected `dev-insecure` provider
- there is no broader production-profile enforcement layer yet
- cloud-KMS and PKCS#11 uplift paths are still future work
