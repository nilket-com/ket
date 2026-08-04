# Impl Evidence 071: Worker + Observability-Access Audit Expansion

**Date:** 2026-05-22
**Base:** `0938704 plan: adr 071 worker observability audit expansion`

## Summary

ADR 071 is implemented. Toron now emits audit events for the two ADR 066
deferrals: worker task lifecycle and observability `ready`/`metrics` access.
The implementation extends the existing Toron audit consumer and shared
`observability::audit` substrate; it does not add a parallel audit facade.

Worker task audit uses `toron.worker.task.{started,completed,failed,cancelled}`
with the new `task_lifecycle` event class. The runtime worker receives the
configured daemon audit emitter, while existing constructors and fixture workers
default to `ToronAuditEmitter::disabled()`. `started` is emitted only after
decode, route validation, task registration, and cancel-before-start acceptance;
terminal recording is one-shot, so each accepted task emits exactly one of
`completed`, `failed`, or `cancelled`. Failure events use bounded
`error_class` values derived from `QueryError` / `TaskFailureKind`.

Observability access audit uses
`toron.observability.{ready,metrics}.access` with the new
`observability_access` event class. The audit hook is Toron network policy:
`ready` and `metrics` handlers record bounded endpoint, method, status, auth,
and outcome classes, plus only the admitted monitor principal as
`actor_key_id=monitor:<principal>`. `/health` remains unaudited. Shared
`observability::{http,audit}` crates remain policy-free.

ADR 066 fail-open semantics are preserved. Audit emission still uses the
nonblocking `try_send` path; queue-full, closed-emitter, and sink failures do
not fail a worker task, `ready`/`metrics` response, or TLS accept path.
`audit.fail_closed` remains out of scope.

## Fixture Coverage

The ADR 071 named fixtures are present and green:

- `worker_task_success_emits_started_and_completed_audit`
- `worker_task_failure_emits_failed_with_bounded_error_class`
- `worker_task_cancelled_before_start_emits_cancelled_without_started`
- `worker_task_cancelled_during_execution_emits_single_cancelled_terminal_event`
- `worker_audit_disabled_is_noop`
- `worker_audit_queue_full_records_drop_without_failing_task`
- `worker_audit_node_id_validation`
- `observability_ready_allowed_emits_access_audit`
- `observability_ready_tls_failure_emits_denied_access_audit`
- `observability_metrics_allowed_emits_access_audit`
- `observability_metrics_method_not_allowed_emits_denied_access_audit`
- `observability_health_does_not_emit_audit`
- `observability_access_records_admitted_monitor_principal_only`
- `observability_access_audit_omits_raw_san_dn_ip_and_certificate_material`

The historical ADR 059 fixture name
`cancel_task_before_registration_tombstones_and_prevents_execution` remains as a
live wrapper around the same cancel-before-start behavior, so the prior guard
continues to pass while ADR 071 gains its own fixture name.

## Verification

- `cargo fmt --all -- --check` - passed.
- `cargo test -p toron adr071_ -- --nocapture` - passed: 21 ADR 071 guards.
- `cargo test -p toron worker_task_ -- --nocapture` - passed: 6 matched tests,
  including the worker task lifecycle audit fixtures.
- `cargo test -p toron worker_audit_ -- --nocapture` - passed: 3 matched tests.
- `cargo test -p toron observability_ -- --nocapture` - passed, including 12
  observability listener tests and the ADR 071 observability architecture guards.
- `cargo check -p toron` - passed.
- `cargo check -p toron --features telemetry` - passed.
- `cargo check -p toron --features otlp-http` - passed.
- `cargo check -p toron --features otlp-grpc` - passed.
- `cargo check -p toron --all-features` - passed.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `cargo test -p toron --test architecture_test` - passed: 886 passed.
- `cargo test -p toron` - passed, including 341 lib tests, 886 architecture
  guards, 33 observability listener tests, 11 passed / 1 ignored Wendelon
  storage execution tests, and doc-tests.
- `git diff --check` - passed.

## Notes

- The ADR 066 worker-no-audit guard was retargeted: runtime `ToronWorker` audit
  is now required, while the fixture-worker, SQL-wire, Wendelon, nilket, and
  shared-observability-policy-free guards remain green.
- The observability TLS helper returns only the admitted monitor principal; audit
  records do not include raw SANs, subject DNs, peer IPs, cert serials, headers,
  PEM bodies, or certificate material.
- At evidence time `df -h .` reported the build filesystem at `1.8T` size,
  `1.7T` used, `35G` available, and `99%` use.
