# Impl Evidence 069: Toron Table-Query Trace Consumer

**Date:** 2026-05-22
**Base:** `e91d268 plan: adr 069 toron table query trace consumer`

## Summary

ADR 069 is implemented. Toron now populates Wendelon ADR 074's
`TableQueryOptions.trace_context` carrier at the table-query storage boundary.
The consumer remains Toron-home: production edits are limited to
`crates/toron/src/telemetry.rs` and `crates/toron/src/storage.rs`; Wendelon
production remains unchanged.

The storage-scan continuation is exposed through the narrow semantic helper
`storage_scan_trace_context(&ScanRequest)`, while `storage_scan_span_id` stays
private. `WendelonStorageScan` uses a disabled-first three-state mapping:
disabled telemetry sends `Absent`, enabled unsampled requests send
`Unsampled`, and enabled sampled requests send `Sampled` with Wendelon parented
on the Toron storage-scan span id, not the incoming task span id.

The ADR 068/074 producer-era guards were retargeted so ADR 074 still owns the
Wendelon carrier/producer surface and ADR 069 owns the Toron consumer
population. ADR 069 guards pin the single shared options value, parentage,
disabled-wins semantics, private hash recipe, cancellation hook shape, table
query read-path-only scope, and no Wendelon production drift.

## Verification

- `cargo fmt` - passed.
- `git diff --check` - passed.
- `cargo test -p toron adr069` - passed: 3 storage seam tests and 19 ADR 069
  architecture guards matched.
- `cargo test -p toron adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074`
  - passed: 1 retargeted ADR 068 guard.
- `cargo test -p wendelon adr074_no_toron_production_change` - passed: 1
  retargeted ADR 074 guard.
- `cargo test -p wendelon --test table_query_telemetry` - passed: 4 passed.
- `cargo test -p toron --test architecture_test` - passed: 845 passed.
- `cargo test -p wendelon --test architecture_test` - passed: 110 passed.
- `cargo check -p toron` - passed.
- `cargo check -p toron --no-default-features` - passed.
- `cargo check -p toron --no-default-features --features telemetry` - passed.
- `cargo check -p toron --no-default-features --features otlp-http` - passed.
- `cargo check -p toron --no-default-features --features otlp-grpc` - passed.
- `cargo test -p toron --test wendelon_storage_execution_test` - passed:
  11 passed, 1 ignored.
- `cargo test -p wendelon --test otlp_http_export` - passed as an unfeatured
  target with 0 tests, because the fixture is `cfg(feature = "otlp-http")`.
- `cargo test -p wendelon --test otlp_grpc_export` - passed as an unfeatured
  target with 0 tests, because the fixture is `cfg(feature = "otlp-grpc")`.
- `cargo test -p wendelon --features otlp-http --test otlp_http_export` -
  passed: 1 passed.
- `cargo test -p wendelon --features otlp-grpc --test otlp_grpc_export` -
  passed: 2 passed.
- `cargo test -p toron` - passed, including the 332 lib tests, 845
  architecture guards, all Toron integration suites, 11 passed / 1 ignored
  Wendelon storage execution tests, and doc-tests.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.

## Notes

- Clippy required the carrier to be populated through a struct update
  initializer rather than default-then-field-reassign. The guard still pins the
  load-bearing invariant: exactly one shared `TableQueryOptions` value carries
  `trace_context` before both Wendelon call paths.
- At evidence time `df -h .` reported the build filesystem at `1.8T` size,
  `1.7T` used, `38G` available, and `98%` use.
