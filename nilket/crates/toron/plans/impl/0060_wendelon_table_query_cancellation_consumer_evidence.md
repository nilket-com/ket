# ADR 060 Implementation Evidence: Wendelon Table-Query Cancellation Consumer

Date: 2026-05-21

## Summary

ADR 060 is implemented in the current worktree. Toron now actively cancels
in-flight Wendelon `TABLE_QUERY_REQ` reads when the parent Toron query is
cancelled, closing the ADR 059 -> ADR 073 -> ADR 060 cancellation chain:

- The Wendelon client exposes a table-query attempt hook that surfaces the
  concrete `(NodeId, TenantId, TableQueryRequestId)` chosen by the lower retry
  loop after request-id generation and before the request write.
- The hook returns an RAII guard held by the streaming query result, so retry,
  success, error, redirect, and drop paths remove exactly the registered
  attempt.
- Toron registers surfaced attempts in a parent-owned downstream attempt
  registry. Parent cancellation triggers snapshot and fire bounded detached
  Wendelon cancels; scans do not generate request ids, watch themselves, or
  fire cancel from drop.
- The ADR 059 active query and active worker task cancellation states own the
  downstream registry, so statement timeout, client disconnect, SQL cancel, and
  worker `CancelTask` share the same downstream path.
- `ExecutionContext` / `ScanRequest` carries optional downstream cancellation
  context. Wendelon scans consume it; in-memory scans ignore it.
- Wendelon `QUERY_CANCELLED` / interrupted table-query cancellation maps to
  `StorageError::Cancelled`, then to Toron cancellation surfaces rather than
  internal, resource, descriptor-refresh, or unavailable errors.
- The new metric
  `toron_downstream_storage_cancellations_total{trigger,outcome}` is
  aggregate-only and identity-free.

The implementation does not add new Toron SQL wire, worker wire,
Control-Ring, transport, SQL grammar, Wendelon producer, writes, CDC, admin,
aggregate, or legacy query cancellation scope.

## Implementation Anchors

- `crates/wendelon/src/client/core.rs`
  - `TableQueryAttempt`, `TableQueryAttemptHook`, and
    `TableQueryAttemptGuard`.
  - `table_query_with_attempt_hook` calls the hook after attempt
    `TableQueryRequestId` generation and before `TABLE_QUERY_REQ` write.
  - `StreamingQueryResult` holds the returned guard.
- `crates/wendelon/crates/wendelon-client/src/lib.rs`
  - Facade hook methods for table query streams and single-query collection.
  - `cancel_table_query_attempt` exposes the ADR 073 cancel producer.
- `crates/toron/src/distributed/cancellation.rs`
  - `DownstreamAttemptRegistry` / `DownstreamAttemptGuard`.
  - `WendelonTableQueryCancelSender` abstraction implemented by the Wendelon
    client.
  - Parent-fired bounded fanout with aggregate outcomes.
  - Active query and active worker task registries owning downstream state.
- `crates/toron/src/exec.rs` and `crates/toron/src/storage.rs`
  - Optional downstream cancellation context threaded to scans.
  - Wendelon scan registers surfaced attempts and checks already-cancelled
    context before issuing a request.
  - In-memory scan ignores the context.
- `crates/toron/src/server/mod.rs`, `crates/toron/src/coordinator.rs`, and
  `crates/toron/src/daemon.rs`
  - Statement timeout, client disconnect, SQL cancel, and worker `CancelTask`
    fire downstream cancellation from the parent state.
- `crates/toron/src/error.rs` and `crates/toron/src/network/observability.rs`
  - `StorageError::Cancelled` mapping and aggregate downstream cancellation
    metric rendering.

## Fixtures And Guards

- Toron architecture guards include the full ADR 060 set: 20 decision guards,
  3 convention guards, and a prior-ADR meta guard scoped
  `..._except_retarget`.
- ADR 059's no-Wendelon-storage-cancellation guard is retargeted to the
  honest residual: Toron now cancels Wendelon table-query reads via ADR 073,
  best-effort and checkpoint-bounded, with no broader Wendelon storage
  cancellation claim.
- Wendelon ADR 073 architecture guards are retargeted to keep Wendelon the
  producer/wire owner while allowing the ADR 060 Toron consumer.
- Unit and integration fixtures cover:
  - statement timeout cancels a dispatched Wendelon attempt;
  - SQL cancel cancels same-tenant Wendelon attempts only;
  - worker `CancelTask` fires downstream before task future drop;
  - cancel-before-scan sends no table-query request;
  - redirect retry replaces attempt registration;
  - RAII removes attempts on success, error, redirect, and drop;
  - cancel racing terminal completion is accepted or terminal without error;
  - downstream fanout timeout does not delay the SQL cancel response;
  - in-memory scan ignores downstream context;
  - Wendelon `QUERY_CANCELLED` maps to Toron cancelled;
  - downstream cancellation metrics are aggregate trigger/outcome only.

## Verification

All cargo/git commands below passed on 2026-05-21. The `rg` audit command
is listed with its expected no-match result below.

```text
cargo fmt -p toron -p wendelon-client -p wendelon
cargo check -p wendelon-client -p toron --tests
cargo clippy -p toron --all-targets -- -D warnings
cargo clippy -p wendelon -p wendelon-client --all-targets -- -D warnings
cargo test -p toron --test architecture_test -- --test-threads=1
cargo test -p toron distributed::cancellation::tests -- --test-threads=1
cargo test -p toron storage::tests -- --test-threads=1
cargo test -p toron --test observability_listener_test downstream_cancellation_metrics_are_aggregate_trigger_outcome_only -- --test-threads=1
cargo test -p toron --test observability_listener_test toron_metrics_endpoint_emits_prometheus_text_format_with_brand_clean_names -- --test-threads=1
cargo test -p wendelon-client -- --test-threads=1
cargo test -p toron -- --test-threads=6
cargo test -p wendelon --test architecture_test adr073 -- --test-threads=1
cargo test -p wendelon -- --test-threads=6
git diff --check
```

Observed focused counts:

- Full Toron architecture suite: 629 passed.
- Toron downstream cancellation unit group: 13 passed.
- Toron storage unit group: 9 passed.
- Downstream metrics fixture: 1 passed.
- Live metrics endpoint retarget fixture: 1 passed.
- Wendelon client package: 3 passed, 0 doc-tests.
- Full Toron package: passed with `--test-threads=6`, including doc-tests.
- Wendelon ADR073 architecture retarget group: 23 passed.
- Full Wendelon package: passed with `--test-threads=6`, including doc-tests.
- `rg -n "tokio::time::sleep" crates/toron/src/server crates/toron/src/distributed`
  exited 1 with no matches, preserving the no-sleep-mock guard.
- `git diff --check`: passed.
