# Release Artifact Evidence

Date: 2026-04-23
Owner: compliance/program + release owner
Status: Repeatable evidence scripts present; no final release artifact checked in

## Artifact Contract

A regulated-profile release evidence directory must contain:

| File | Purpose | Produced by |
|---|---|---|
| `wendelon` | release binary | `ci/generate_regulated_release_evidence.sh` |
| `wendelon.sha256` | SHA-256 digest for the binary | `ci/generate_regulated_release_evidence.sh` |
| `cargo-metadata.json` | locked dependency graph input | `cargo metadata --locked` |
| `sbom.spdx.json` | SPDX SBOM | `cargo-sbom`, when installed |
| `provenance.json` | local provenance statement with profile, git revision, dirty state, UTC build time, embedded binary digest, and either `attestation_path` or `attestation_missing` | `ci/generate_regulated_release_evidence.sh` |
| `release-manifest.sha256` | SHA-256 manifest for the binary, provenance, dependency metadata, and any present SBOM/attestation | `ci/generate_regulated_release_evidence.sh` |
| `wendelon.sig` | detached SHA-256 artifact signature | `openssl`, when `WENDELON_RELEASE_SIGNING_KEY` is set |
| `provenance.json.sig` | detached SHA-256 provenance signature | `openssl`, when `WENDELON_RELEASE_SIGNING_KEY` is set |
| `release-manifest.sha256.sig` | detached SHA-256 release-manifest signature | `openssl`, when `WENDELON_RELEASE_SIGNING_KEY` is set |
| `attestation.intoto.jsonl` | in-toto/SLSA-style attestation statement | external release attestation step |

Missing optional release prerequisites are recorded as `.missing` files so the gap is visible instead of silently skipped.

## Generation

```bash
ci/generate_regulated_release_evidence.sh fips target/regulated_release/fips
```

The script builds:

```bash
cargo build --locked --profile fips --no-default-features --features fips --bin wendelon
```

For the default profile:

```bash
ci/generate_regulated_release_evidence.sh default target/regulated_release/default
```

## Verification

```bash
ci/verify_regulated_release_evidence.sh fips target/regulated_release/fips
```

Set these variables when a release gate requires hard failure instead of visible missing markers:

| Variable | Effect |
|---|---|
| `REQUIRE_SPDX_SBOM=1` | fail if `sbom.spdx.json` is missing |
| `REQUIRE_RELEASE_SIGNING=1` | fail if the binary, provenance, and release-manifest signatures are missing |
| `REQUIRE_ATTESTATION=1` | fail if `attestation.intoto.jsonl` is missing |
| `WENDELON_RELEASE_VERIFY_KEY=/path/to/pubkey.pem` | verify the binary, provenance, and release-manifest signatures cryptographically |

## Current Status

The scripts are present and checked by release-evidence tests. This is not the same as a completed signed release: no authoritative SBOM, detached signature set, or attestation output is checked in for a released binary yet. The `build_time_utc` field is generated from the local release runner clock; external timestamping/notarization remains a later release-control decision.
