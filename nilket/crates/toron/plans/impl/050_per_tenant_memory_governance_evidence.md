# ADR 050 Evidence: Per-Tenant Memory Governance

**ADR:** `crates/toron/plans/adr/050_per_tenant_memory_governance.md`
**Implementation plan:** `crates/toron/plans/impl/050_per_tenant_memory_governance.md`

## Implementation Notes

- `[tenant_budgets]` is node-local Toron config with a required default when present and optional per-tenant overrides.
- Omitted `[tenant_budgets]` synthesizes a default policy from `QueryBudget::default().memory_per_task` and `[worker].memory_budget_bytes`, preserving existing loopback and unlisted-tenant behavior.
- Query memory continues to flow through the existing `QueryBudget` / `TaskBudget` path; `MemoryManager` and `BufPool` remain wire-buffer surfaces.
- `TenantMemoryAccountant` reserves each query's effective tenant memory up front at coordinator admission and releases by RAII on success, error, and early return.
- Admission rejection happens before bind/execute and maps to `SqlErrorCode::ResourceExhausted` with the stable SQL diagnostic `tenant memory budget exhausted`.
- Downstream distributed task memory failures also map to `ResourceExhausted`; no new distributed wire message or task error variant was added.

## Preserved Boundaries

- Tenant identity is sourced only from ADR 049 execution context: authenticated client SAN for remote SQL, configured tenant fallback for loopback SQL.
- No CPU quotas, rate limits, connection quotas, storage quotas, tenant queues, borrowing, spilling, or fairness scheduler.
- No per-tenant metric labels.
- No production changes under `crates/wendelon`.
- `crates/nilket/Cargo.toml` remains runtime-free for Toron.
- `crates/toron/src/bin/toron_fixture_worker.rs` remains source-unchanged.
- New protocol/config/code surface is brand-clean and has no V1 suffix.

## Verification

Commands run:

```text
cargo fmt --all
cargo test -p toron --lib
cargo test -p toron --test config_test tenant_budget -- --nocapture
cargo test -p toron --test architecture_test adr050 -- --nocapture
cargo test -p toron --test sql_listener_test query_over_per_task_budget_returns_resource_exhausted -- --nocapture
cargo test -p toron --test architecture_test
cargo test -p toron --test config_test --test sql_listener_test --test tls_listener_test --test toron_daemon_test --test toron_observability_listener_test
cargo test -p toron --quiet
cargo clippy -p toron -- -D warnings
```

Observed results:

- `cargo test -p toron --lib`: 224 passed.
- `architecture_test adr050`: 16 passed.
- Full `architecture_test`: 419 passed.
- `config_test`: 13 passed.
- `sql_listener_test`: 8 passed.
- `tls_listener_test`: 10 passed.
- `toron_daemon_test`: 5 passed.
- `toron_observability_listener_test`: 11 passed.
- Full `cargo test -p toron --quiet`: passed all Toron suites, including 224 lib tests, 419 architecture guards, all integration suites, and the existing Wendelon-backed storage fixture suite with 9 passed / 1 ignored.
- `cargo clippy -p toron -- -D warnings`: passed.

## Fixture Notes

- `tenant_budget_policy_config_round_trips` verifies default and per-tenant budget parsing.
- `tenant_budget_config_rejects_zero_duplicate_and_per_task_over_aggregate` verifies zero, duplicate, and per-task-over-aggregate rejection.
- `distributed_coordinator_selects_query_budget_by_tenant` verifies tenant-specific per-query budget selection while preserving other budget dimensions.
- `aggregate_admission_rejects_before_bind_or_execution` verifies aggregate rejection happens before catalog binding.
- `aggregate_reservation_released_on_successful_completion` verifies coordinator-level RAII release after a successful admitted DDL query.
- `aggregate_reservation_released_on_error` verifies RAII release on query failure.
- `one_tenant_saturated_does_not_block_another` verifies tenant-isolated aggregate saturation.
- `query_over_per_task_budget_returns_resource_exhausted` verifies a live SQL query exceeding the configured memory budget returns `SqlErrorCode::ResourceExhausted` and the stable diagnostic.
