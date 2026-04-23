# Workstream J Self-Test Evidence

Date: 2026-04-23
Owner: `J`
Status: Startup checks implemented; lab KAT plan still pending

## Startup Sequence

The FIPS-profile startup path is:

1. `src/node.rs` calls `build_info::initialize_fips_runtime()` before the node reports readiness.
2. `src/build_info.rs` calls `aws_lc_rs::try_fips_mode()`.
3. `src/build_info.rs` runs Wendelon startup known-answer tests.
4. On success in the `fips` build, the node records `fips_mode=true`, stores `self_test_last_ts`, and increments `wendelon_fips_module_self_test_total{outcome="ok"}`.
5. On failure, node startup returns an `std::io::Error` and the node does not become ready.

## Known-Answer Tests

| Test | Vector | Expected result |
|---|---|---|
| SHA-256 | input `abc` | `ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad` |
| AES-256-GCM | zero key, zero nonce, empty AAD, empty plaintext | tag `530f8afbc74536b9a963b4f1c4cb738b` |

The AES-256-GCM KAT seals and opens the vector so both directions are exercised through `aws-lc-rs`.

## Failure Mode

Failure is terminal for startup. The error string uses:

```text
FIPS startup self-test failed at <stage>: <detail>
```

The aws-lc-rs FIPS-mode failure path uses:

```text
aws-lc-rs FIPS mode unavailable: <detail>
```

There is no runtime downgrade from `fips` to `default`.

## Evidence Commands

```bash
cargo test --lib build_info::tests
CC=clang CXX=clang++ cargo test --lib --no-default-features --features fips build_info::tests
cargo test --test compliance_release_contract
ci/check_fips_symbol_sweep.sh
```

The first command runs the startup KAT unit tests in the default test profile. The second command compiles and runs the same test module under the `fips` feature profile. The symbol sweep is the FIPS-profile binary guard and requires a local toolchain capable of building `aws-lc-rs` with the `fips` feature.

## Remaining Work

- Lab-approved operational self-test language is not written yet.
- The repo does not yet include a forced failure test against a real aws-lc-rs FIPS-mode failure; the deterministic failure check covers the Wendelon error mapping only.
- Conditional tests required by the eventual CMVP lab are not finalized.
