# Impl Evidence 066: Toron Audit Emission Consumer

**Date:** 2026-05-21
**Base:** `e97364e plan: adr 066 audit emission consumer`

## Summary

ADR 066 is implemented. Toron now owns its audit event vocabulary in
`crates/toron/src/audit.rs` while consuming the shared `observability::audit`
substrate added by ADR 065. The `observability` feature enables both HTTP and
audit substrate support for Toron, but `[audit]` remains default-disabled.

The new Toron audit consumer validates `toron-n-...` node identifiers, opens a
local chain only after config validation succeeds, and runs fail-open emission
through a bounded queue. Queue-full and queue-closed drops increment aggregate
audit counters without degrading readiness. Local chain open/write failures
are treated as security-event-unrecorded conditions and degrade the process
readiness state.

SQL listener lifecycle, edge-denial, and query-outcome points now emit typed
Toron audit events without raw SQL text, table names, tenant labels in metrics,
query IDs in metrics, or session IDs in metrics. Oversized request frames and
oversized statements emit edge-denial audit events before the SQL frame path
returns. Planned errors, successful queries, and cancellation outcomes emit
query-outcome events with bounded statement, duration, row-count, and error-code
classes.

The ADR 066 architecture guards pin the Toron-owned vocabulary, shared-substrate
separation, default-disabled config surface, local-chain lifecycle, fail-open
queue semantics, readiness behavior, aggregate-only metrics, SQL listener
emission points, and the ADR 045 / ADR 065 retargets.

## Verification

- `CARGO_BUILD_JOBS=1 cargo check -p toron --tests` - passed.
- `CARGO_BUILD_JOBS=1 cargo clippy -p toron --all-targets -- -D warnings` -
  passed.
- `CARGO_BUILD_JOBS=1 cargo test -p toron audit -- --test-threads=1` - passed
  across the audit-filtered unit, architecture, config, observability, and SQL
  listener fixtures.
- `CARGO_BUILD_JOBS=1 cargo test -p toron --test architecture_test adr066 -- --test-threads=1`
  - passed: 21 passed, matching the reconciled ADR 066 guard count.
- `CARGO_BUILD_JOBS=1 cargo test -p toron --test architecture_test -- --test-threads=1`
  - passed: 771 passed, including all ADR 066 guards.
- `CARGO_BUILD_JOBS=1 cargo test -p toron --test sql_listener_test -- --test-threads=6`
  - passed: 20 passed.
- `CARGO_BUILD_JOBS=1 cargo test -p toron --test config_test -- --test-threads=1`
  - passed: 39 passed.
- `CARGO_BUILD_JOBS=1 cargo test -p toron --test observability_listener_test -- --test-threads=1`
  - passed: 26 passed.
- `CARGO_BUILD_JOBS=1 cargo test -p toron -- --test-threads=6` - passed; the
  pre-existing Wendelon admin fixture remains ignored.

## Notes

- The full Toron test run exercised loopback and mTLS SQL listener fixtures.
  Expected negative-handshake, QUIC connection-close, and OpenRaft shutdown log
  lines were present while the tests passed.
