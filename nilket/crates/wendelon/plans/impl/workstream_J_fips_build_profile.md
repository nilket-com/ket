# Workstream J FIPS Build-Profile Note

Date: 2026-04-23
Owner: `J`
Status: Implemented profile, incomplete certification posture

## Profile Contract

The regulated build path is a distinct Cargo profile plus a distinct feature set:

```bash
cargo build --locked --profile fips --no-default-features --features fips --bin wendelon
```

The ordinary release path remains:

```bash
cargo build --locked --release --bin wendelon
```

`Cargo.toml` defines `[profile.fips]` inheriting the release hardening settings. `ci/check_fips_symbol_sweep.sh` now builds that profile directly and inspects `target/fips/wendelon`.

## Compile-Time Restrictions

- `src/crypto.rs` emits a compile error if `fips` and `non-fips` are both enabled.
- `src/crypto.rs` emits a compile error if neither crypto profile is enabled.
- In the `fips` feature profile, `detect_cipher_once()` selects `CipherId::Aes256GcmInjective`.
- In the `fips` feature profile, `algorithm_for_cipher(CipherId::Aes256GcmSiv)` returns `Unsupported`; AES-256-GCM-SIV is not an active FIPS-profile path.

## Startup Restrictions

`src/build_info.rs` runs the regulated startup checks before node readiness:

- `aws_lc_rs::try_fips_mode()` must succeed in the `fips` build.
- Wendelon startup KATs must pass for SHA-256 and AES-256-GCM.
- `src/node.rs` aborts startup if `initialize_fips_runtime()` returns an error.
- Successful FIPS startup increments `wendelon_fips_module_self_test_total{outcome="ok"}` in the `fips` build.
- FIPS-profile readiness exposes `fips_mode=true` and `fips_self_test_last_ts`.

## Dependency Pinning

The release command uses `--locked`, and `build.rs` emits `WENDELON_AWS_LC_RS_VERSION` from `Cargo.lock`. The current locked versions are:

| Dependency | Locked version |
|---|---:|
| `aws-lc-rs` | `1.16.3` |
| `aws-lc-fips-sys` | `0.13.14` |
| `aws-lc-sys` | `0.40.0` |

Any dependency bump that touches these packages must update `plans/impl/workstream_J_crypto_boundary_inventory.md` and rerun the FIPS symbol sweep.

## Current Certification Gaps

This profile is not enough to claim FIPS certification. Current gaps are explicit:

- The repo has no CMVP certificate and no lab-selected boundary record.
- Transport TLS still depends on `rustls`/`ring` and is outside the current at-rest boundary.
- Ed25519 signing paths still exist in the FIPS-profile binary unless later scoped out or moved.
- `local-sealed` unlock derivation uses `scrypt`; certified-profile custody treatment is still unresolved.
- Release SBOM, detached signature, and attestation outputs are scripted but not yet checked in as real release evidence.

## Required Checks

```bash
cargo test --lib build_info::tests
CC=clang CXX=clang++ cargo test --lib --no-default-features --features fips build_info::tests
bash -n ci/check_fips_symbol_sweep.sh
ci/check_fips_symbol_sweep.sh
```

The full symbol sweep can require a local C/C++ toolchain compatible with `aws-lc-rs` FIPS builds.

## Current Validation

On 2026-04-23, the FIPS profile compiled and the symbol sweep passed from a clean target directory with:

```bash
CARGO_TARGET_DIR=/tmp/wendelon-fips-check CC=clang CXX=clang++ ci/check_fips_symbol_sweep.sh
```

The script defaults to `clang`/`clang++` because the default `/usr/bin/cc` path failed inside the `aws-lc-fips-sys` CMake/delocation step in this workspace.
