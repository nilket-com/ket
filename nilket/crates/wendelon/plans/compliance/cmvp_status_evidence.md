# CMVP Status Evidence

Date: 2026-04-23
Owner: `J` + `A`
Status: Open external dependency

## Current Status

Wendelon is not FIPS-certified, not CMVP validated, and not represented in this repo as being in validation.

The repo-visible state is:

- FIPS build-profile work exists.
- Crypto-boundary inventory exists.
- Startup self-test/KAT code exists.
- Symbol-sweep and release-evidence scripts exist.
- No CMVP certificate exists in the repo.
- No selected lab or lab acceptance record exists in the repo.

The external tracker remains the source of truth for lab contact state:

- `plans/governance/external_dependency_tracker.md`
- `plans/compliance/external/cmvp_lab_contact_record.md`

## Allowed Claim Language

Allowed:

- "Wendelon has a regulated-release workstream with a FIPS build profile under implementation."
- "The repo tracks a crypto-boundary inventory and current certification gaps."
- "The current build profile is not a CMVP certificate."

Not allowed:

- "FIPS-certified"
- "FIPS compliant"
- "CMVP validated"
- "in validation"
- "lab approved"
- "certification pending" unless the external lab record names the lab and accepted status

## Evidence Required To Move This Row

The status cannot move beyond `partial` until the external evidence record includes:

- lab shortlist or selected lab
- contact date and current response
- module/binary boundary submitted for review
- exact claim language approved by the compliance/program owner
- certificate number, if certification completes

## Boundary Caveats For Lab Scoping

The first lab conversation must explicitly handle these current facts:

- transport TLS uses `rustls`/`ring` and is outside the current at-rest boundary
- Ed25519 signing code is present today
- `local-sealed` uses `scrypt` for unlock-secret derivation
- default-profile BLAKE3 remains outside the intended FIPS boundary
- the FIPS build profile selects AES-256-GCM with injective nonces and excludes AES-256-GCM-SIV from active at-rest encryption
