# Impl Evidence 065: `crates/observability` Namespace and Audit Substrate

**Date:** 2026-05-21
**Base:** `19cf91b plan: adr 065 observability namespace audit substrate`

## Summary

ADR 065 is implemented. The flat `crates/observability-http` runtime crate has
been removed and its ADR 048 HTTP helper is now `observability::http` inside
the new `crates/observability` namespace crate. Toron and Wendelon now consume
that helper through the `observability` crate, with Toron enabling only the
`http` feature.

Wendelon's audit mechanics are extracted into `observability::audit` while the
Wendelon schema and operation vocabulary remain ring-owned in
`crates/wendelon/src/audit.rs`. The shared audit substrate owns the local
NDJSON chain, chain replay/verification, signed chain-root helpers, sink
configs, archive/syslog/Kafka projections, and composite-sink mechanics.
Wendelon provides the `AuditFormat` profile for schema version 4, chain/root
tags, syslog identifiers, canonical operation labels, and node-id decoding; no
Wendelon constants or ring types are hard-coded in the shared crate.

The guard set was retargeted for ADR 048 and expanded for ADR 065:
`observability` has `default=[]`, independent `http`/`audit` feature builds,
no source references between the two modules, no Toron/Wendelon runtime
dependency from the shared crate, Kafka remains feature-gated, Toron has no
audit emission/config yet, and `observability::telemetry` is reserved for a
future ADR.

## Verification

- `cargo fmt --check` - passed.
- `cargo check -p observability --no-default-features --features http` -
  passed.
- `cargo check -p observability --no-default-features --features audit` -
  passed.
- `cargo check -p observability --no-default-features --features kafka` -
  passed.
- `cargo check -p wendelon --features audit-kafka --lib` - passed.
- `cargo clippy -p observability --all-targets --no-default-features --features http,audit -- -D warnings`
  - passed.
- `cargo clippy -p wendelon --all-targets -- -D warnings` - passed.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `cargo test -p observability --no-default-features --features http,audit` -
  passed: 4 audit substrate tests and 9 HTTP substrate tests.
- `cargo test -p toron --test architecture_test -- --test-threads=1` -
  passed: 750 passed, including all ADR 065 guards and the ADR 048 retarget.
- `cargo test -p wendelon --test architecture_test -- --test-threads=1` -
  passed: 90 passed.
- `cargo test -p wendelon audit -- --test-threads=1` - passed across the
  audit-filtered Wendelon test binaries.
- `cargo test -p toron -- --test-threads=6` - passed; one pre-existing
  Wendelon admin fixture remains ignored.
- `git diff --check` - passed.

## Notes

- The full Toron test run exercised loopback and mTLS SQL listener fixtures.
  Expected negative-handshake, QUIC connection-close, and OpenRaft shutdown
  log lines were present while the tests passed.
- Full `cargo test -p wendelon` was not run; the Wendelon coverage for this
  migration was the architecture suite, audit-filtered behavioral tests, full
  `--all-targets` clippy, and the `audit-kafka` feature check.
