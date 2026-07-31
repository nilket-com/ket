# ADR 059 Implementation Evidence: Distributed Query Cancellation Completeness

Date: 2026-05-21

## Summary

ADR 059 is implemented in the current worktree. Toron distributed query
cancellation now has a real worker-side abort path instead of an ack-only
`CancelTask` surface:

- SQL statement timeout, SQL client disconnect after `Execute` decode, and
  SQL `Cancel(CancelRequest)` route through one tenant-scoped cancellation
  handle keyed by `(TenantId, QueryId)`.
- Worker `CancelTask` signals an active task keyed by `TaskIdentity`, or
  records a bounded pending tombstone for the cancel-before-`StartTask` race.
- Worker task execution races the inline task future against the cancellation
  signal, suppresses pre-start/pre-publish success surfaces when cancellation
  is already observed, and runs partition cleanup for cancelled query output.
- Cancellation waiters arm `tokio::sync::Notify` with `Notified::enable()`
  before the final cancelled-state check, closing the lost-wakeup race for the
  prompt abort path.
- Remote cleanup uses a fresh bidirectional worker stream and the existing
  `CancelTask` / `TaskCancelled` wire. Client SQL responses do not wait for
  remote cancel acknowledgements.
- Cancellation metrics are aggregate-only with `trigger` and `outcome`
  labels; no query/task/worker/table/tenant/SQL-text/endpoint labels.

The implementation remains Toron-only. It does not add textual SQL
`CANCEL`, a new SQL wire variant, `ControlMessage`, unidirectional streams,
a worker control endpoint, or any Wendelon storage cancellation claim.

## Implementation Anchors

- `crates/toron/src/distributed/cancellation.rs`
  - `ActiveTaskRegistry` with RAII deregistration, bounded TTL/count
    pending-cancel tombstones, and task cancellation signals.
  - `ActiveQueryRegistry` keyed by `ActiveQueryKey { tenant_id, query_id }`.
  - Cancellation waiters that are robust to pre-wait and between-check
    cancellation.
  - `CancellationTrigger`, `CancellationOutcome`, and aggregate
    `CancellationMetrics`.
- `crates/toron/src/server/mod.rs`
  - `StartTask` registers before visible task execution, races execution
    against cancellation, suppresses pre-start/pre-publish success paths, and
    emits `TaskFailureKind::Cancelled`.
  - `CancelTask` decodes the existing payload, signals the active registry,
    runs local partition cleanup, and returns `TaskCancelled`.
- `crates/toron/src/coordinator.rs`
  - Distributed execution registers an active query after admission and passes
    the handle into the network dispatcher.
  - `cancel_query_for_tenant` is the single idempotent cancellation handle for
    timeout, disconnect, and SQL cancel.
  - Remote fanout is internally bounded and uses `cancel_task_on_worker` over
    the existing bidirectional stream.
- `crates/toron/src/daemon.rs`
  - Statement timeout and client disconnect cancel before returning.
  - SQL `Cancel` now returns `SqlErrorCode::Cancelled` for accepted,
    missing, terminal, and tenant-invisible ids.
- `crates/toron/src/network/observability.rs`
  - `query_cancellations_total{trigger, outcome}` renders aggregate
    cancellation counters only.

## Fixtures And Guards

- Architecture guards added/retargeted in `tests/architecture_test.rs` for:
  existing cancel wire reuse, no control-protocol drift, tenant-scoped active
  query identity, worker active registry/tombstones, drop-at-await
  suppression, bounded aggregate remote-fanout outcomes, timeout-before-return,
  disconnect-before-decode no-op cancellation, no pre-admission cancellation,
  local cleanup on remote fanout failure, concurrent cancel stream handling,
  no Wendelon storage cancellation claim, the ADR 059 convention trio, and the
  ADR 059 prior-ADR meta guard.
- Unit fixtures cover active-task tombstone bounds/consumption, active-task
  RAII deregistration, tenant-scoped active-query idempotence, remote
  `CancelTask` fanout with `TaskCancelled` ack, remote fanout error metrics,
  pre-admission rejection without cancellation fanout, admission/memory release
  after cancellation, pre-registration tombstone suppression of task execution,
  and pre-wait cancellation observation for both worker and query handles.
- SQL listener fixture retargets the existing cancel-frame cap test to
  `SqlErrorCode::Cancelled`.
- Observability fixture verifies aggregate cancellation trigger/outcome
  metrics and forbidden identity/text labels.
- Prior ADR guards retargeted narrowly where ADR 059 added the active query
  handle argument, preserving the original admission and distributed-source
  invariants.

## Verification

All commands below passed on 2026-05-21:

```text
cargo check -p toron --tests
cargo clippy -p toron --all-targets -- -D warnings
cargo test -p toron --test architecture_test adr059 -- --test-threads=1
cargo test -p toron --test architecture_test -- --test-threads=1
cargo test -p toron distributed::cancellation -- --test-threads=1
cargo test -p toron remote_cancel_fanout_uses_existing_wire_and_records_ack_metric -- --test-threads=1
cargo test -p toron remote_cancel_fanout_records_error_when_connect_fails -- --test-threads=1
cargo test -p toron pre_admission_rejects_emit_no_cancel_task -- --test-threads=1
cargo test -p toron admission_and_memory_release_after_cancellation -- --test-threads=1
cargo test -p toron cancel_task_before_registration_tombstones_and_prevents_execution -- --test-threads=1
cargo test -p toron --test observability_listener_test cancellation_metrics_are_aggregate_trigger_outcome_only -- --test-threads=1
cargo test -p toron --test sql_listener_test cancel_below_request_frame_cap_is_not_statement_capped -- --test-threads=1
cargo test -p toron -- --test-threads=6
```

Observed counts from the focused gates:

- `architecture_test adr059`: 28 passed.
- Full `architecture_test`: 605 passed.
- `distributed::cancellation`: 5 passed.
- `remote_cancel_fanout_uses_existing_wire_and_records_ack_metric`: 1 passed.
- `remote_cancel_fanout_records_error_when_connect_fails`: 1 passed.
- `pre_admission_rejects_emit_no_cancel_task`: 1 passed.
- `admission_and_memory_release_after_cancellation`: 1 passed.
- `cancel_task_before_registration_tombstones_and_prevents_execution`: 1 passed.
- `cancellation_metrics_are_aggregate_trigger_outcome_only`: 1 passed.
- `cancel_below_request_frame_cap_is_not_statement_capped`: 1 passed.
- Full `cargo test -p toron -- --test-threads=6`: passed, including Toron
  doc-tests and the Wendelon storage execution suite's expected ignored
  schema-update fixture.
