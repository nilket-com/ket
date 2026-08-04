# ADR 052 Evidence: Per-Tenant Concurrency Admission and Bounded Resource Observability

**ADR:** `crates/toron/plans/adr/0052_per_tenant_concurrency_admission.md`
**Implementation plan:** `crates/toron/plans/impl/0052_per_tenant_concurrency_admission.md`

## Implementation Notes

- `[tenant_budgets]` default and per-tenant entries now accept optional
  `max_concurrent_queries`; omitted values derive from
  `max(1, aggregate_memory_bytes / memory_per_task_bytes)` and explicit
  zero values fail with the ADR 052 diagnostic.
- `TenantBudget` carries `max_concurrent_queries: NonZeroU32`.
  `TenantBudgetPolicy::from_query_budget` keeps the conservative
  single-query fallback for bare coordinators.
- `TenantConcurrencyAccountant` / `TenantConcurrencyReservation` are
  tenant-keyed, count-based, and RAII-released beside the ADR 050
  memory accountant. They use a dedicated `TenantConcurrencyRejection`
  rather than overloading byte-oriented budget rejection types.
- `DistributedQueryCoordinator::admit_query` reserves concurrency first,
  then memory, before bind/planning/catalog/execution. Memory failure
  after concurrency admission drops the concurrency reservation before
  returning.
- Concurrency exhaustion maps through `ExecutionError::ConcurrencyExhausted`
  to the existing `SqlErrorCode::ResourceExhausted` with the stable SQL
  diagnostic `tenant concurrency limit exhausted`.
- `MetricsRegistry` renders bounded resource-governance metrics for
  configured tenant ids plus the `"other"` bucket:
  `tenant_memory_bytes_used`, `tenant_memory_bytes_limit`,
  `tenant_concurrency_active`, `tenant_concurrency_limit`, and
  `tenant_admission_rejections_total{resource="memory|concurrency"}`.
  General daemon/listener metrics remain aggregate.
- Admission rejection counters are relaxed-atomic and incremented on
  memory/concurrency admission failures; current usage gauges render from
  accountant snapshots.

## Preserved Boundaries

- Tenant identity is still sourced only from ADR 049 execution context:
  authenticated client SAN for remote SQL, configured tenant fallback for
  loopback SQL.
- No CPU wall-time limits, rate limits, connection caps, storage quotas,
  queueing, borrowing, fair-share scheduling, or cluster-wide semaphore.
- No SQL wire change and no new SQL error code.
- No descriptor sync, reload endpoint, file watch, runtime descriptor
  ingestion, or Wendelon production change.
- `crates/nilket/Cargo.toml` remains runtime-free for Toron.
- `crates/toron/src/bin/toron_fixture_worker.rs` remains source-unchanged.
- New ADR 052 surface is brand-clean and has no `*V1` suffix.

## Verification

Commands run:

```text
cargo fmt
cargo check -p toron
cargo test -p toron --no-run
cargo test -p toron tenant_concurrency
cargo test -p toron adr052
cargo test -p toron resource_metrics
cargo test -p toron sql_concurrency
cargo test -p toron --test architecture_test
cargo clippy -p toron --all-targets -- -D warnings
cargo test -p toron
```

Observed results:

- `cargo check -p toron`: passed.
- `cargo test -p toron --no-run`: compiled all Toron test targets.
- `cargo test -p toron tenant_concurrency`: 9 targeted tests passed
  (budget, coordinator admission, config parsing/validation).
- `cargo test -p toron adr052`: 18 ADR 052 architecture guards passed.
- `cargo test -p toron resource_metrics`: 4 targeted resource-metric
  tests passed.
- `cargo test -p toron sql_concurrency`: 1 SQL error-mapping test passed.
- Full `architecture_test`: 457 passed.
- `cargo clippy -p toron --all-targets -- -D warnings`: passed. This
  required cleaning the previously exposed all-target clippy nits in older
  tests/benches as well as the ADR 052 metrics helper warning.
- Full `cargo test -p toron`: passed all Toron suites; the
  Wendelon-backed storage execution suite reported 9 passed / 1 ignored,
  matching the existing ignored fixture.

## Fixture Notes

- `tenant_concurrency_policy_config_round_trips`,
  `tenant_concurrency_config_rejects_zero`, and
  `tenant_concurrency_omitted_config_derives_default` pin the config
  surface, nonzero validation, and derived default.
- `tenant_concurrency_accountant_releases_on_drop` and
  `tenant_concurrency_accountant_isolated_by_tenant` pin the accountancy
  primitive.
- `tenant_concurrency_limit_rejects_before_bind`,
  `tenant_concurrency_reservation_releases_on_success`,
  `tenant_concurrency_reservation_releases_on_error`,
  `memory_rejection_releases_concurrency_reservation`, and
  `one_tenant_concurrency_saturation_does_not_block_another` pin
  admission ordering, RAII release, and tenant isolation.
- `sql_concurrency_exhaustion_returns_resource_exhausted` pins SQL error
  code reuse and the concurrency-specific diagnostic.
- `resource_metrics_label_configured_tenant`,
  `resource_metrics_bucket_unconfigured_tenant_as_other`,
  `resource_metrics_expose_memory_and_concurrency_limits`,
  `resource_admission_rejections_are_labeled_by_resource`, and
  `general_daemon_metrics_do_not_gain_tenant_labels` pin the bounded
  metric-label rule and the aggregate general/listener metric boundary.
