# Impl Evidence 072: `audit.fail_closed` Enforcement

**Date:** 2026-05-22
**Base:** `368fd3b plan: adr 072 audit fail closed`

## Summary

ADR 072 is implemented. Toron keeps ADR 066/071 audit emission fail-open by
default, while `[audit] fail_closed = true` enables required audit gating for
security-relevant SQL, worker, and observability surfaces.

The audit emitter now has two APIs:

- `record(event)`: synchronous, nonblocking, `try_send`, fail-open.
- `record_required(event).await`: mode-aware; delegates to `record` and returns
  `Ok(())` when fail-closed is off, and waits for an audit-worker one-shot ack
  when fail-closed is on.

Required ack success is sent only after the shared local audit sink returns
success. The shared sink writes the local chain record and calls `sync_data()`.
Queue-full, closed-emitter, serialization failure, sink failure, and ack timeout
map to bounded `AuditUnavailable` reasons. Sink-failure diagnostics are always
best-effort and never recursively required.

Config defaults preserve 071 behavior: absent/disabled audit and
`fail_closed=false` are fail-open. `fail_closed=true` with `audit.enabled=false`
is startup-invalid, and `fail_closed_ack_timeout_ms` is nonzero with a 500 ms
default.

## Surface Coverage

- SQL edge denials, terminal query outcomes, cancellation, timeout, disconnect,
  and unexpected client frames use required audit before the protected response
  or close point.
- SQL success pre-encodes result batches under fail-closed, performs required
  terminal audit, and only then writes result batches / `Complete`.
- Worker start audit is required before `TaskStarted` and before task execution.
  Worker terminal audit is required before `TaskComplete` or terminal
  `TaskFailed`; audit failure uses the existing `TaskFailed` wire shape with
  bounded `audit_unavailable`.
- Observability `ready` and `metrics` access audit is required before the
  response; audit unavailability returns `503` with `audit unavailable\n`.
- `/health`, lifecycle audit, and audit sink-failure diagnostics remain
  best-effort.

No new SQL-wire, worker-wire, storage, Wendelon, nilket, or shared
`observability::audit` policy surface was added.

## Verification

- `cargo fmt --all` - run.
- `cargo fmt --all --check` - passed.
- `cargo check -p toron --all-targets` - passed.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `cargo test -p toron --test architecture_test adr072_ -- --nocapture` -
  passed: 22 ADR 072 guards.
- `cargo test -p toron --test architecture_test adr071_ -- --nocapture` -
  passed: 21 ADR 071 guards after retargeting the stale no-fail-closed guard.
- `cargo test -p toron --test architecture_test adr066_ -- --nocapture` -
  passed: 21 ADR 066 guards after retargeting the command-wrapper shape.
- `cargo test -p toron --test architecture_test -- --nocapture` - passed: 908
  guards.
- `cargo test -p toron --test config_test -- --nocapture` - passed: 41 tests.
- `cargo test -p toron --lib -- --nocapture` - passed: 349 tests.
- `cargo test -p toron --test observability_listener_test -- --nocapture` -
  passed: 34 tests.
- `cargo test -p toron --test sql_listener_test -- --nocapture` - passed: 21
  tests.
- `cargo test -p observability --features audit -- --nocapture` - passed: 4
  shared audit substrate tests.
- `git diff --check` - passed.
- Focused fail-closed fixtures also passed:
  `record_required_delegates_fail_open_when_fail_closed_false`,
  `required_audit_*`, `sink_failure_diagnostic_does_not_recursively_require_audit`,
  `worker_started_audit_failure_prevents_task_execution`,
  `fail_closed_sql_success_audit_failure_returns_internal_before_result_batches`,
  and `observability_metrics_audit_failure_returns_503_without_sink_detail`.

## Notes

- The SQL no-result-batches runtime fixture uses the same daemon audit emitter as
  the self-worker, so an unavailable fail-closed emitter prevents worker
  execution first. The architecture guard pins the SQL success-path ordering
  directly: batch pre-encode, required terminal audit, then result batch writes.
- At evidence time `df -h .` reported the build filesystem at `1.8T` size,
  `1.7T` used, `34G` available, and `99%` use.
